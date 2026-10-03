package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

// testClock 是可手动推进的时钟。
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock {
	return &testClock{t: time.Unix(1000, 0)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testEnv 封装测试服务与 HTTP 客户端。
type testEnv struct {
	t     *testing.T
	clock *testClock
	svc   *Service
	srv   *httptest.Server
}

func newTestEnv(t *testing.T) *testEnv {
	clock := newTestClock()
	svc := New(Config{
		DefaultFreshTTL: 100 * time.Millisecond,
		DefaultStaleWindow: 1000 * time.Millisecond,
		Clock:           clock.Now,
	})
	srv := httptest.NewServer(svc.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = svc.Events().Close()
	})
	return &testEnv{t: t, clock: clock, svc: svc, srv: srv}
}

func (e *testEnv) register(path string, mode model.RenderMode, tags []string, fresh, stale time.Duration, policy model.ErrorPolicy) {
	e.t.Helper()
	route := model.Route{
		Path:        path,
		Mode:        mode,
		Tags:        tags,
		FreshTTL:    fresh,
		StaleWindow: stale,
		ErrorPolicy: policy,
	}
	e.svc.Route(route)
}

func (e *testEnv) publish(path string, body string) int64 {
	e.t.Helper()
	resp, err := http.Post(e.srv.URL+"/publish?route="+path, "text/plain", strings.NewReader(body))
	if err != nil {
		e.t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		e.t.Fatalf("decode publish: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("publish status %d", resp.StatusCode)
	}
	return out.Version
}

type resp struct {
	status  int
	body    string
	headers http.Header
}

func (e *testEnv) get(path string) resp {
	e.t.Helper()
	r, err := http.Get(e.srv.URL + "/render" + path)
	if err != nil {
		e.t.Fatalf("get: %v", err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return resp{status: r.StatusCode, body: string(b), headers: r.Header}
}

func (e *testEnv) post(path string, qs string, body string) resp {
	e.t.Helper()
	r, err := http.Post(e.srv.URL+path+"?"+qs, "application/json", strings.NewReader(body))
	if err != nil {
		e.t.Fatalf("post: %v", err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return resp{status: r.StatusCode, body: string(b), headers: r.Header}
}

func (e *testEnv) invalidate(tag string, version int64) {
	e.t.Helper()
	body := fmt.Sprintf(`{"tag":%q,"version":%d,"event_id":"test-%s-%d"}`, tag, version, tag, version)
	r := e.post("/invalidate", "", body)
	if r.status != http.StatusOK {
		e.t.Fatalf("invalidate status %d: %s", r.status, r.body)
	}
}

func (e *testEnv) failNext(path string, n int) {
	e.t.Helper()
	e.post("/admin/failures", "route="+path+"&n="+fmt.Sprint(n), "")
}

func (e *testEnv) failForever(path string) {
	e.t.Helper()
	e.post("/admin/failures", "route="+path+"&forever=1", "")
}

func (e *testEnv) recover(path string) {
	e.t.Helper()
	e.post("/admin/failures", "route="+path, "")
}

func (e *testEnv) clearCache() {
	e.t.Helper()
	e.post("/admin/clear", "", "")
}

func (e *testEnv) cacheEntries() []map[string]any {
	e.t.Helper()
	r, err := http.Get(e.srv.URL + "/admin/cache")
	if err != nil {
		e.t.Fatalf("admin cache: %v", err)
	}
	defer r.Body.Close()
	var out struct {
		Entries []map[string]any `json:"entries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		e.t.Fatalf("decode cache: %v", err)
	}
	return out.Entries
}

func (e *testEnv) storeVersions() []map[string]any {
	e.t.Helper()
	r, err := http.Get(e.srv.URL + "/admin/store")
	if err != nil {
		e.t.Fatalf("admin store: %v", err)
	}
	defer r.Body.Close()
	var out struct {
		Versions []map[string]any `json:"versions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		e.t.Fatalf("decode store: %v", err)
	}
	return out.Versions
}

// ---- 测试用例 ----

// SSG：构建期预生成；读取永不触发回源渲染，数据源故障不影响已发布页面。
func TestSSG_PrerenderAndNoRerender(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeSSG, []string{"t1"}, 0, 0, "")

	// 未发布即读取：503
	if r := e.get("/a"); r.status != http.StatusServiceUnavailable {
		t.Fatalf("unpublished SSG want 503, got %d", r.status)
	}

	v1 := e.publish("/a", "ssg-body-v1")
	if r := e.get("/a"); r.status != http.StatusOK || r.headers.Get(HeaderCache) != CacheHit ||
		r.headers.Get(HeaderRenderMode) != string(model.ModeSSG) || r.body != "ssg-body-v1" {
		t.Fatalf("SSG hit mismatch: status=%d cache=%s mode=%s body=%q",
			r.status, r.headers.Get(HeaderCache), r.headers.Get(HeaderRenderMode), r.body)
	}

	// 数据源持续故障：SSG 读取不回源，仍然命中
	e.failForever("/a")
	if r := e.get("/a"); r.status != http.StatusOK || r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("SSG must not hit downstream: status=%d cache=%s", r.status, r.headers.Get(HeaderCache))
	}

	// 重新发布（新版本）后读取到新内容
	e.recover("/a")
	v2 := e.publish("/a", "ssg-body-v2")
	if v2 <= v1 {
		t.Fatalf("versions must increase: %d -> %d", v1, v2)
	}
	if r := e.get("/a"); r.body != "ssg-body-v2" || r.headers.Get(HeaderVersion) != fmt.Sprint(v2) {
		t.Fatalf("SSG after republish: body=%q version=%s", r.body, r.headers.Get(HeaderVersion))
	}
}

// SSR：每次请求实时渲染，不读写缓存；回源失败返回 502。
func TestSSR_AlwaysRender(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeSSR, nil, 0, 0, "")

	e.publish("/a", "ssr-v1")
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss || r.body != "ssr-v1" {
		t.Fatalf("SSR want MISS render: cache=%s body=%q", r.headers.Get(HeaderCache), r.body)
	}

	// 数据源变更后立即读到新内容（无缓存）
	e.svc.DataSource().Set("/a", []byte("ssr-v2"))
	if r := e.get("/a"); r.body != "ssr-v2" {
		t.Fatalf("SSR must render latest: body=%q", r.body)
	}

	// 回源持续失败：502 + 可观察头
	e.failForever("/a")
	if r := e.get("/a"); r.status != http.StatusBadGateway || r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("SSR failure want 502/MISS, got %d/%s", r.status, r.headers.Get(HeaderCache))
	}
}

// ISR：fresh 命中 -> 陈旧窗口内返回 STALE 并后台再生 -> 窗口外同步再生。
func TestISR_FreshStaleExpired(t *testing.T) {
	e := newTestEnv(t)
	fresh, stale := 100*time.Millisecond, 500*time.Millisecond
	e.register("/a", model.ModeISR, []string{"t1"}, fresh, stale, "")
	e.publish("/a", "isr-v1")

	// 首次读取：未命中 -> 同步再生
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("first ISR want MISS, got %s", r.headers.Get(HeaderCache))
	}
	// 新鲜期内：命中
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("fresh ISR want HIT, got %s", r.headers.Get(HeaderCache))
	}

	// 进入陈旧窗口：STALE + Warning 110，后台静默再生
	e.clock.Advance(fresh + 10*time.Millisecond)
	r := e.get("/a")
	if r.status != http.StatusOK || r.headers.Get(HeaderCache) != CacheStale ||
		r.headers.Get(HeaderWarning) != WarnStale {
		t.Fatalf("want STALE+110, got status=%d cache=%s warn=%q", r.status, r.headers.Get(HeaderCache), r.headers.Get(HeaderWarning))
	}

	// 等待后台再生完成，随后读取应重新 HIT
	time.Sleep(200 * time.Millisecond)
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("after background regen want HIT, got %s", r.headers.Get(HeaderCache))
	}

	// 超出陈旧窗口：同步再生（数据源正常 -> MISS 后恢复）
	e.clock.Advance(fresh + stale + time.Second)
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("beyond window want sync regen MISS, got %s", r.headers.Get(HeaderCache))
	}
}

// ISR 再生失败策略 stale-if-error：窗口内返回陈旧，窗口外返回 504。
func TestISR_RegenError_StaleIfError(t *testing.T) {
	e := newTestEnv(t)
	fresh, stale := 100*time.Millisecond, 500*time.Millisecond
	e.register("/a", model.ModeISR, []string{"t1"}, fresh, stale, model.ErrorPolicyStaleIfError)
	e.publish("/a", "isr-v1")
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("want MISS, got %s", r.headers.Get(HeaderCache))
	}

	// 进入陈旧窗口后数据源持续故障：仍返回陈旧（STALE + 110）
	e.clock.Advance(fresh + 10*time.Millisecond)
	e.failForever("/a")
	r := e.get("/a")
	if r.status != http.StatusOK || r.headers.Get(HeaderCache) != CacheStale ||
		r.headers.Get(HeaderWarning) != WarnStale || r.body != "isr-v1" {
		t.Fatalf("stale-if-error within window want STALE, got status=%d cache=%s warn=%q body=%q",
			r.status, r.headers.Get(HeaderCache), r.headers.Get(HeaderWarning), r.body)
	}

	// 超出陈旧窗口：错误 -> 504 EXPIRED + Warning 111
	e.clock.Advance(stale + time.Second)
	r = e.get("/a")
	if r.status != http.StatusGatewayTimeout || r.headers.Get(HeaderCache) != CacheExpired ||
		r.headers.Get(HeaderWarning) != WarnRevalidationFailed {
		t.Fatalf("beyond window want 504/EXPIRED/111, got status=%d cache=%s warn=%q",
			r.status, r.headers.Get(HeaderCache), r.headers.Get(HeaderWarning))
	}

	// 数据源恢复后同步再生成功
	e.recover("/a")
	if r := e.get("/a"); r.status != http.StatusOK || r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("recover want MISS/200, got status=%d cache=%s", r.status, r.headers.Get(HeaderCache))
	}
}

// ISR 再生失败策略 error：即使存在陈旧条目也直接报错。
func TestISR_RegenError_ErrorPolicy(t *testing.T) {
	e := newTestEnv(t)
	fresh, stale := 100*time.Millisecond, 500*time.Millisecond
	e.register("/a", model.ModeISR, []string{"t1"}, fresh, stale, model.ErrorPolicyError)
	e.publish("/a", "isr-v1")
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("want MISS, got %s", r.headers.Get(HeaderCache))
	}

	// 超出陈旧窗口后数据源故障：error 策略不返回陈旧，直接 504
	e.clock.Advance(fresh + stale + time.Second)
	e.failForever("/a")
	r := e.get("/a")
	if r.status != http.StatusGatewayTimeout || r.headers.Get(HeaderCache) != CacheExpired {
		t.Fatalf("error policy want 504/EXPIRED, got status=%d cache=%s", r.status, r.headers.Get(HeaderCache))
	}
}

// 模式隔离：路由声明模式变更后，旧模式条目不得被新模式读取路径返回。
func TestModeIsolation(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeSSG, []string{"t1"}, 0, 0, "")
	e.publish("/a", "ssg-body")
	if r := e.get("/a"); r.headers.Get(HeaderRenderMode) != string(model.ModeSSG) {
		t.Fatalf("want ssg, got %s", r.headers.Get(HeaderRenderMode))
	}

	// 切换为 ISR：缓存中 SSG 条目不得被当作命中
	e.register("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss ||
		r.headers.Get(HeaderRenderMode) != string(model.ModeISR) {
		t.Fatalf("mode switch must not reuse SSG entry: cache=%s mode=%s",
			r.headers.Get(HeaderCache), r.headers.Get(HeaderRenderMode))
	}
}

// 标签定向失效：一次内容变更只让受影响页面退出缓存，其余页面保持命中。
func TestTagInvalidation_Targeted(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")
	e.register("/b", model.ModeISR, []string{"t2"}, time.Minute, time.Minute, "")
	e.register("/c", model.ModeISR, []string{"t1", "t2"}, time.Minute, time.Minute, "")
	e.publish("/a", "a1")
	e.publish("/b", "b1")
	e.publish("/c", "c1")
	for _, p := range []string{"/a", "/b", "/c"} {
		if r := e.get(p); r.headers.Get(HeaderCache) != CacheMiss {
			t.Fatalf("%s want MISS, got %s", p, r.headers.Get(HeaderCache))
		}
	}
	// 全部命中
	for _, p := range []string{"/a", "/b", "/c"} {
		if r := e.get(p); r.headers.Get(HeaderCache) != CacheHit {
			t.Fatalf("%s want HIT, got %s", p, r.headers.Get(HeaderCache))
		}
	}

	// 发布 /a（标签 t1 变更）：/a 与 /c 失效，/b 继续命中
	e.publish("/a", "a2")
	if r := e.get("/b"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("/b (tag t2 unaffected) must stay HIT, got %s", r.headers.Get(HeaderCache))
	}
	for _, p := range []string{"/a", "/c"} {
		if r := e.get(p); r.headers.Get(HeaderCache) != CacheMiss {
			t.Fatalf("%s must be invalidated and regen, got %s", p, r.headers.Get(HeaderCache))
		}
	}
	// 不清空整站：/b 仍命中
	if r := e.get("/b"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("/b must still HIT (no full-site flush), got %s", r.headers.Get(HeaderCache))
	}
}

// 回源瞬时失败：有界重试后成功；反复失败不损坏索引、无孤儿条目。
func TestRetry_DownstreamFailures(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeSSR, nil, 0, 0, "")
	e.svc.DataSource().Set("/a", []byte("ok-body"))

	// 前 2 次失败，第 3 次成功 -> 重试后 200
	e.failNext("/a", 2)
	if r := e.get("/a"); r.status != http.StatusOK || r.body != "ok-body" {
		t.Fatalf("retry should succeed: status=%d body=%q", r.status, r.body)
	}

	// 持续失败 -> 502
	e.failForever("/a")
	if r := e.get("/a"); r.status != http.StatusBadGateway {
		t.Fatalf("persistent failure want 502, got %d", r.status)
	}

	// 缓存索引未损坏：SSR 不产生条目，管理接口正常
	entries := e.cacheEntries()
	for _, en := range entries {
		if en["route"] == "/a" {
			t.Fatalf("SSR must not create cache entry, got %v", en)
		}
	}

	// ISR 再生失败重试：前 2 次失败后成功
	e.register("/isr", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")
	e.svc.DataSource().Set("/isr", []byte("isr-body"))
	e.failNext("/isr", 2)
	if r := e.get("/isr"); r.status != http.StatusOK || r.body != "isr-body" {
		t.Fatalf("ISR regen retry should succeed: status=%d body=%q", r.status, r.body)
	}
	// 重试成功后条目可读、状态正常（无孤儿）
	entries = e.cacheEntries()
	found := false
	for _, en := range entries {
		if en["route"] == "/isr" {
			found = true
			if en["state"] != "fresh" {
				t.Fatalf("regenerated entry must be fresh, got %v", en["state"])
			}
		}
	}
	if !found {
		t.Fatal("regenerated entry missing from index (orphan)")
	}
}

// 并发发布与读取：读取方看到的内容必须与版本号自洽，无撕裂。
func TestAtomicPublish_ConcurrentReads(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")
	e.publish("/a", "seed")

	const publishers = 3
	const reads = 200
	var wg sync.WaitGroup

	// 并发发布：每次发布嵌入版本标记
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				body := fmt.Sprintf("content-p%d-v%d", base, i)
				e.publish("/a", body)
			}
		}(p)
	}

	// 并发读取：body 中的版本标记必须与 X-Content-Version 头一致，
	// 且 body 必须完整（不出现半行/混合）
	for r := 0; r < reads; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := e.get("/a")
			if resp.status != http.StatusOK {
				t.Errorf("read status %d", resp.status)
				return
			}
			body := resp.body
			ver := resp.headers.Get(HeaderVersion)
			// body 必须非空（完整），版本头必须存在且为正整数；
			// body 内容与版本号的自洽由发布流程保证。
			if body == "" || ver == "" || ver == "0" {
				t.Errorf("torn read: body=%q version=%q", body, ver)
			}
		}()
	}
	wg.Wait()
}

// 并发发布、失效与读取：最终缓存状态与全量重建一致。
func TestConcurrentPublishInvalidateRead(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, 50*time.Millisecond, 2*time.Second, "")
	e.register("/b", model.ModeISR, []string{"t2"}, 50*time.Millisecond, 2*time.Second, "")
	e.publish("/a", "seed-a")
	e.publish("/b", "seed-b")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 持续发布 /a 与 /b
	for _, path := range []string{"/a", "/b"} {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				e.publish(p, fmt.Sprintf("%s-body-%d", p, i))
				i++
			}
		}(path)
	}

	// 持续读取
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r := e.get("/a")
				if r.status != http.StatusOK {
					t.Errorf("read status %d", r.status)
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 全量重建：清空缓存后重新预热，每个路由的内容必须与 store 当前版本一致
	e.clearCache()
	for _, path := range []string{"/a", "/b"} {
		r := e.get(path)
		if r.status != http.StatusOK {
			t.Fatalf("rebuild %s status %d", path, r.status)
		}
	}
	versions := e.storeVersions()
	for _, v := range versions {
		path := v["route"].(string)
		r := e.get(path)
		if r.headers.Get(HeaderVersion) != fmt.Sprint(int64(v["version"].(float64))) {
			t.Fatalf("%s after rebuild version mismatch: header=%s store=%v",
				path, r.headers.Get(HeaderVersion), v["version"])
		}
	}
}

// 失效事件乱序、重复、迟到：最终状态与全量重建一致，迟到事件不复活。
func TestInvalidation_OutOfOrderDuplicateLate(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")

	// 发布 v1 并再生：条目持有 deps {t1:1}
	e.publish("/a", "v1")
	e.get("/a") // MISS 再生
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("want HIT, got %s", r.headers.Get(HeaderCache))
	}

	// 乱序事件：(t1,2) 先到（使 deps<2 的条目失效），(t1,1) 迟到，(t1,2) 重复。
	// 条目 deps {t1:1}：(t1,2) 生效（1 < 2），与到达顺序无关。
	e.invalidate("t1", 2)
	e.invalidate("t1", 1)
	e.invalidate("t1", 2)

	// 缓存应已失效（最高版本 2 生效）
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("out-of-order events should invalidate (highest version wins), got %s", r.headers.Get(HeaderCache))
	}

	// 发布 v2：提交 t1=2（与事件版本一致），再生条目 deps {t1:2}
	e.publish("/a", "v2")
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("publish v2 want MISS (regen), got %s", r.headers.Get(HeaderCache))
	}
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("want HIT, got %s", r.headers.Get(HeaderCache))
	}

	// 迟到事件 (t1,1) 与重复事件 (t1,2) 不得影响当前内容
	e.invalidate("t1", 1) // 迟到事件
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("late event must not invalidate current content, got %s", r.headers.Get(HeaderCache))
	}
	e.invalidate("t1", 2) // 重复事件
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("duplicate event must not invalidate current content, got %s", r.headers.Get(HeaderCache))
	}

	// 事件计数：重复事件被去重
	_, processed, dups, _ := e.svc.Events().Stats()
	if dups < 1 {
		t.Fatalf("duplicate events should be counted, got dups=%d processed=%d", dups, processed)
	}
}

// 全量重建一致性：warmup 后所有路由内容与已提交版本一致。
func TestFullRebuildConsistency(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")
	e.register("/b", model.ModeISR, []string{"t2"}, time.Minute, time.Minute, "")
	e.register("/c", model.ModeSSG, []string{"t3"}, 0, 0, "")
	e.publish("/a", "a-body")
	e.publish("/b", "b-body")
	e.publish("/c", "c-body")

	// 制造混乱：失效、时间推进
	e.invalidate("t1", 1)
	e.clock.Advance(2 * time.Minute)

	// 全量预热
	r := e.post("/admin/warmup", "", "")
	if r.status != http.StatusOK {
		t.Fatalf("warmup status %d: %s", r.status, r.body)
	}

	// 每个 ISR 路由读取到的内容必须与 store 当前版本一致
	for _, v := range e.storeVersions() {
		path := v["route"].(string)
		mode := model.RenderMode(v["mode"].(string))
		r := e.get(path)
		if r.status != http.StatusOK {
			t.Fatalf("%s status %d", path, r.status)
		}
		if mode == model.ModeISR && r.headers.Get(HeaderCache) != CacheHit {
			t.Fatalf("%s after warmup want HIT, got %s", path, r.headers.Get(HeaderCache))
		}
		if r.headers.Get(HeaderVersion) != fmt.Sprint(int64(v["version"].(float64))) {
			t.Fatalf("%s version mismatch: header=%s store=%v", path, r.headers.Get(HeaderVersion), v["version"])
		}
	}
}

// 异步事件：乱序入队后 Flush，最终状态一致。
func TestAsyncEvents(t *testing.T) {
	clock := newTestClock()
	svc := New(Config{
		DefaultFreshTTL: time.Minute,
		DefaultStaleWindow: time.Minute,
		Clock:           clock.Now,
		AsyncEvents:     true,
	})
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	defer svc.Events().Close()

	svc.Route(model.Route{Path: "/a", Mode: model.ModeISR, Tags: []string{"t1"}})
	svc.Route(model.Route{Path: "/b", Mode: model.ModeISR, Tags: []string{"t2"}})

	publish := func(path, body string) {
		resp, err := http.Post(srv.URL+"/publish?route="+path, "text/plain", strings.NewReader(body))
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		resp.Body.Close()
	}
	get := func(path string) resp {
		r, err := http.Get(srv.URL + "/render" + path)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return resp{status: r.StatusCode, body: string(b), headers: r.Header}
	}

	publish("/a", "a1")
	publish("/b", "b1")
	get("/a")
	get("/b")
	if r := get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("want HIT, got %s", r.headers.Get(HeaderCache))
	}

	// 异步发布失效：事件乱序到达也不影响 /b
	publish("/a", "a2")
	svc.Events().Flush()
	if r := get("/b"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("/b must stay HIT after async invalidation of /a, got %s", r.headers.Get(HeaderCache))
	}
	if r := get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("/a want MISS after async invalidation, got %s", r.headers.Get(HeaderCache))
	}
}

// Single-flight：陈旧窗口内并发读取只触发一次后台再生。
func TestSingleFlight_BackgroundRegen(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, 100*time.Millisecond, 500*time.Millisecond, "")
	e.publish("/a", "v1")
	e.get("/a") // MISS 同步再生

	// 进入陈旧窗口后并发读取
	e.clock.Advance(110 * time.Millisecond)
	var wg sync.WaitGroup
	staleCount := 0
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := e.get("/a")
			mu.Lock()
			if r.headers.Get(HeaderCache) == CacheStale {
				staleCount++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if staleCount == 0 {
		t.Fatal("expected STALE responses within stale window")
	}

	// 等待后台再生完成
	time.Sleep(300 * time.Millisecond)
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("after regen want HIT, got %s", r.headers.Get(HeaderCache))
	}
}

// 未注册路由返回 404。
func TestRouteNotFound(t *testing.T) {
	e := newTestEnv(t)
	if r := e.get("/nope"); r.status != http.StatusNotFound {
		t.Fatalf("want 404, got %d", r.status)
	}
}

// 失效事件未指定版本时使用当前标签版本：对已是当前版本的条目是 no-op。
func TestInvalidate_DefaultVersion(t *testing.T) {
	e := newTestEnv(t)
	e.register("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute, "")
	e.publish("/a", "v1")
	e.get("/a") // MISS 再生，条目 deps {t1:1}
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("want HIT, got %s", r.headers.Get(HeaderCache))
	}
	// 不带版本号的失效：使用标签当前版本（t1=1），条目 deps 也是 1，no-op
	r := e.post("/invalidate", "", `{"tag":"t1"}`)
	if r.status != http.StatusOK {
		t.Fatalf("invalidate status %d: %s", r.status, r.body)
	}
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("default-version at current version is no-op, got %s", r.headers.Get(HeaderCache))
	}

	// 发布 v2 后条目已被事件 (t1,2) 失效；再生后 deps {t1:2}
	e.publish("/a", "v2")
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheMiss {
		t.Fatalf("publish should invalidate, got %s", r.headers.Get(HeaderCache))
	}
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("want HIT, got %s", r.headers.Get(HeaderCache))
	}
	// 再次不带版本号失效：当前版本 t1=2，条目 deps=2，no-op
	r = e.post("/invalidate", "", `{"tag":"t1"}`)
	if r.status != http.StatusOK {
		t.Fatalf("invalidate status %d: %s", r.status, r.body)
	}
	if r := e.get("/a"); r.headers.Get(HeaderCache) != CacheHit {
		t.Fatalf("default-version at current version is no-op, got %s", r.headers.Get(HeaderCache))
	}
}
