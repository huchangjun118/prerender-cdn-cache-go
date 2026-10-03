package prerender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestServer(t *testing.T, origin Origin) (*httptest.Server, *Handler) {
	t.Helper()
	retry := RetryConfig{MaxAttempts: 2, BaseDelay: 0}
	store := NewContentStore()
	renderer := NewRenderer(origin, retry)
	cache := NewCache(NewMemoryBackend(), store, renderer, retry)
	h := NewHandler(cache, store, renderer)
	srv := httptest.NewServer(h.Mux())
	t.Cleanup(srv.Close)
	return srv, h
}

func putRoute(t *testing.T, srv *httptest.Server, route string, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/routes/"+route, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put route %s: %d", route, resp.StatusCode)
	}
}

func getPage(t *testing.T, srv *httptest.Server, route string) *http.Response {
	t.Helper()
	resp, err := http.Get(srv.URL + "/pages/" + route)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func publish(t *testing.T, srv *httptest.Server, route string) *http.Response {
	t.Helper()
	resp, err := http.Post(srv.URL+"/publish/"+route, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func invalidate(t *testing.T, srv *httptest.Server, seq uint64, tag string, version uint64) {
	t.Helper()
	body, _ := json.Marshal(invalidateRequest{Seq: seq, Tag: tag, Version: version})
	resp, err := http.Post(srv.URL+"/invalidate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("invalidate: %d", resp.StatusCode)
	}
}

// 端到端：声明路由 -> 发布 -> 读取命中 -> 定向失效 -> 重新发布，
// 缓存行为全程可从响应头观察。
func TestHTTPEndToEnd(t *testing.T) {
	var n atomic.Int64
	origin := &fakeOrigin{body: func(route string, v uint64) []byte {
		return []byte(fmt.Sprintf("body-%d-%d", v, n.Add(1)))
	}}
	srv, _ := newTestServer(t, origin)

	putRoute(t, srv, "landing", `{"mode":"ssg","tags":["site","landing"]}`)
	putRoute(t, srv, "about", `{"mode":"ssg","tags":["site","about"]}`)

	// 未发布：404 且 X-Cache=ERROR。
	resp := getPage(t, srv, "landing")
	if resp.StatusCode != http.StatusNotFound || resp.Header.Get(HeaderCacheStatus) != string(StatusError) {
		t.Fatalf("unpublished: %d %s", resp.StatusCode, resp.Header.Get(HeaderCacheStatus))
	}
	resp.Body.Close()

	// 发布 v1 后读取：MISS 然后 HIT，版本与模式可从响应头观察。
	resp = publish(t, srv, "landing")
	if resp.StatusCode != http.StatusCreated || resp.Header.Get(HeaderContentVersion) != "1" {
		t.Fatalf("publish: %d", resp.StatusCode)
	}
	resp.Body.Close()
	publish(t, srv, "about").Body.Close()

	// SSG 在发布时已回填缓存，首次读取即 HIT。
	for i := 0; i < 2; i++ {
		resp = getPage(t, srv, "landing")
		if resp.Header.Get(HeaderCacheStatus) != string(StatusHit) ||
			resp.Header.Get(HeaderContentVersion) != "1" ||
			resp.Header.Get(HeaderRenderMode) != string(ModeSSG) {
			t.Fatalf("get %d headers: %v", i, resp.Header)
		}
		resp.Body.Close()
	}

	// 定向失效 landing 标签：landing 退出缓存，about 继续命中。
	invalidate(t, srv, 1, "landing", 1)
	resp = getPage(t, srv, "landing")
	if resp.StatusCode != http.StatusNotFound { // 已提交版本 v1 被失效，无有效版本可服务
		t.Fatalf("invalidated ssg should be 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = getPage(t, srv, "about")
	if resp.Header.Get(HeaderCacheStatus) != string(StatusHit) {
		t.Fatalf("about should keep HIT, got %s", resp.Header.Get(HeaderCacheStatus))
	}
	resp.Body.Close()

	// 重新发布 v2：新版本可正常服务（失效不得丢失新版本）。
	resp = publish(t, srv, "landing")
	if resp.Header.Get(HeaderContentVersion) != "2" {
		t.Fatalf("republish: %v", resp.Header)
	}
	resp.Body.Close()
	resp = getPage(t, srv, "landing")
	if resp.StatusCode != http.StatusOK || resp.Header.Get(HeaderContentVersion) != "2" {
		t.Fatalf("after republish: %d %v", resp.StatusCode, resp.Header)
	}
	resp.Body.Close()
}

// ISR 通过 HTTP 暴露陈旧窗口行为：X-Cache=STALE 可观察。
func TestHTTPISRStaleObservable(t *testing.T) {
	origin := &flakyOrigin{failTimes: 0}
	srv, h := newTestServer(t, origin)
	_ = h

	putRoute(t, srv, "feed", `{"mode":"isr","freshTTLMs":1,"staleTTLMs":60000,"stalePolicy":"serve-stale","tags":["feed"]}`)

	resp := getPage(t, srv, "feed") // MISS，写入缓存
	if resp.Header.Get(HeaderCacheStatus) != string(StatusMiss) {
		t.Fatalf("first: %s", resp.Header.Get(HeaderCacheStatus))
	}
	resp.Body.Close()

	// 等待超过新鲜期后让回源失败：应返回 STALE。
	time.Sleep(5 * time.Millisecond)
	origin.failTimes = 100 // 后续全部失败
	origin.calls.Store(0)
	resp = getPage(t, srv, "feed")
	if resp.Header.Get(HeaderCacheStatus) != string(StatusStale) ||
		resp.StatusCode != http.StatusOK ||
		resp.Header.Get(HeaderContentVersion) != "1" {
		t.Fatalf("stale: %d %v", resp.StatusCode, resp.Header)
	}
	resp.Body.Close()

	// 策略为 fail 的路由：回源失败返回 503 且 X-Cache=ERROR。
	putRoute(t, srv, "strict", `{"mode":"isr","freshTTLMs":0,"staleTTLMs":60000,"stalePolicy":"fail","tags":["strict"]}`)
	resp = getPage(t, srv, "strict")
	if resp.StatusCode != http.StatusServiceUnavailable ||
		resp.Header.Get(HeaderCacheStatus) != string(StatusError) {
		t.Fatalf("fail policy: %d %s", resp.StatusCode, resp.Header.Get(HeaderCacheStatus))
	}
	resp.Body.Close()
}

func TestItoa(t *testing.T) {
	for _, v := range []uint64{0, 1, 9, 10, 123456789} {
		if itoa(v) != strconv.FormatUint(v, 10) {
			t.Fatalf("itoa(%d)=%s", v, itoa(v))
		}
	}
}

var _ = context.Background // 保持 import 稳定
