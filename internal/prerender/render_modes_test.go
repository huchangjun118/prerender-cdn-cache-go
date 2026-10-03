package prerender

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// fakeOrigin 是可编程的下游渲染源。
type fakeOrigin struct {
	calls atomic.Int64
	fail  atomic.Bool
	body  func(route string, version uint64) []byte
}

func (f *fakeOrigin) Render(_ context.Context, route string, version uint64) (*Page, error) {
	f.calls.Add(1)
	if f.fail.Load() {
		return nil, errors.New("origin boom")
	}
	body := []byte(fmt.Sprintf("%s-v%d", route, version))
	if f.body != nil {
		body = f.body(route, version)
	}
	return &Page{Body: body}, nil
}

func newFixture() (*Cache, *ContentStore, *fakeOrigin) {
	origin := &fakeOrigin{}
	retry := RetryConfig{MaxAttempts: 1}
	store := NewContentStore()
	renderer := NewRenderer(origin, retry)
	cache := NewCache(NewMemoryBackend(), store, renderer, retry)
	return cache, store, origin
}

func ssgCfg(route string) RouteConfig {
	return RouteConfig{Path: route, Mode: ModeSSG, Tags: []string{"t-" + route}}
}

func ssrCfg(route string) RouteConfig {
	return RouteConfig{Path: route, Mode: ModeSSR, StalePolicy: StalePolicyServeStale}
}

func isrCfg(route string) RouteConfig {
	return RouteConfig{
		Path:        route,
		Mode:        ModeISR,
		FreshTTL:    time.Minute,
		StaleTTL:    time.Minute,
		StalePolicy: StalePolicyServeStale,
		Tags:        []string{"t-" + route},
	}
}

func TestSSGServesCommittedVersions(t *testing.T) {
	cache, store, origin := newFixture()
	cfg := ssgCfg("a")

	if _, _, err := cache.Get(context.Background(), cfg); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("expected ErrNotPublished, got %v", err)
	}

	store.Commit(&Page{Route: "a", Mode: ModeSSG, Version: 1, Body: []byte("v1"), Tags: cfg.Tags})
	p, st, err := cache.Get(context.Background(), cfg)
	if err != nil || st != StatusMiss || string(p.Body) != "v1" {
		t.Fatalf("first get: p=%v st=%s err=%v", p, st, err)
	}
	if origin.calls.Load() != 0 {
		t.Fatalf("SSG must not call origin at request time, got %d calls", origin.calls.Load())
	}
	if _, st, _ = cache.Get(context.Background(), cfg); st != StatusHit {
		t.Fatalf("second get should HIT, got %s", st)
	}

	// 原子发布新版本后，缓存落后于已提交版本时必须切换到新版本。
	store.Commit(&Page{Route: "a", Mode: ModeSSG, Version: 2, Body: []byte("v2"), Tags: cfg.Tags})
	p, st, _ = cache.Get(context.Background(), cfg)
	if st != StatusMiss || string(p.Body) != "v2" || p.Version != 2 {
		t.Fatalf("after publish: p=%v st=%s", p, st)
	}
	if p, st, _ = cache.Get(context.Background(), cfg); st != StatusHit || string(p.Body) != "v2" {
		t.Fatalf("after refill should HIT v2, got %s %v", st, p)
	}
}

func TestSSRAlwaysRendersAndFallsBackToCommitted(t *testing.T) {
	cache, store, origin := newFixture()
	cfg := ssrCfg("b")

	if _, st, _ := cache.Get(context.Background(), cfg); st != StatusMiss {
		t.Fatalf("SSR first get: %s", st)
	}
	if _, st, _ := cache.Get(context.Background(), cfg); st != StatusMiss {
		t.Fatalf("SSR must render per request, got %s", st)
	}
	if origin.calls.Load() != 2 {
		t.Fatalf("SSR should call origin every request, got %d", origin.calls.Load())
	}

	// 回源失败 + serve-stale：回退到最近已提交版本。
	store.Commit(&Page{Route: "b", Mode: ModeSSR, Version: 1, Body: []byte("committed")})
	origin.fail.Store(true)
	p, st, err := cache.Get(context.Background(), cfg)
	if err != nil || st != StatusStale || string(p.Body) != "committed" {
		t.Fatalf("SSR stale fallback: p=%v st=%s err=%v", p, st, err)
	}

	// 策略为 fail 时直接报错。
	cfg.StalePolicy = StalePolicyFail
	if _, st, err := cache.Get(context.Background(), cfg); err == nil || st != StatusError {
		t.Fatalf("SSR fail policy: st=%s err=%v", st, err)
	}
}

func TestISRRegeneratesAndServesStaleWithinWindow(t *testing.T) {
	cache, _, origin := newFixture()
	cfg := isrCfg("c")
	now := time.Now()
	cache.now = func() time.Time { return now }

	p, st, _ := cache.Get(context.Background(), cfg)
	if st != StatusMiss || p.Version != 1 {
		t.Fatalf("ISR first get: st=%s v=%d", st, p.Version)
	}
	if _, st, _ := cache.Get(context.Background(), cfg); st != StatusHit {
		t.Fatalf("ISR within fresh TTL should HIT, got %s", st)
	}

	// 过期后再生。
	now = now.Add(2 * time.Minute)
	if p, st, _ = cache.Get(context.Background(), cfg); st != StatusMiss || p.Version != 2 {
		t.Fatalf("ISR regen: st=%s v=%d", st, p.Version)
	}

	// 再生失败但处于陈旧窗口内：返回陈旧内容。
	now = now.Add(90 * time.Second) // 超出新鲜期，仍在陈旧窗口
	origin.fail.Store(true)
	p, st, err := cache.Get(context.Background(), cfg)
	if err != nil || st != StatusStale || p.Version != 2 {
		t.Fatalf("ISR stale: p=%v st=%s err=%v", p, st, err)
	}

	// 超出陈旧窗口：报错。
	now = now.Add(2 * time.Minute)
	if _, st, err = cache.Get(context.Background(), cfg); err == nil || st != StatusError {
		t.Fatalf("ISR beyond stale window: st=%s err=%v", st, err)
	}

	// 策略为 fail 时，即使在陈旧窗口内也报错。
	origin.fail.Store(true)
	cfgFail := isrCfg("c2")
	cfgFail.StalePolicy = StalePolicyFail
	if _, st, err = cache.Get(context.Background(), cfgFail); err == nil || st != StatusError {
		t.Fatalf("ISR fail policy: st=%s err=%v", st, err)
	}
}

// 不同模式产出的内容不得串用：ISR 写入的缓存对 SSG 读取不可见，反之亦然。
func TestModesDoNotShareCachedContent(t *testing.T) {
	cache, store, _ := newFixture()

	isr := isrCfg("d")
	pISR, _, err := cache.Get(context.Background(), isr)
	if err != nil {
		t.Fatal(err)
	}

	// 同一路由改声明为 SSG：必须读已提交版本，不得返回 ISR 产出的缓存。
	store.Commit(&Page{Route: "d", Mode: ModeSSG, Version: 7, Body: []byte("ssg-v7"), Tags: isr.Tags})
	p, st, err := cache.Get(context.Background(), ssgCfg("d"))
	if err != nil || st != StatusMiss || string(p.Body) != "ssg-v7" {
		t.Fatalf("SSG must not serve ISR content: p=%v st=%s err=%v", p, st, err)
	}
	if string(pISR.Body) == string(p.Body) {
		t.Fatal("mode content leaked across modes")
	}

	// 再切回 ISR：SSG 回填的条目 mode 为 ssg，不得作为 ISR 命中。
	p, st, err = cache.Get(context.Background(), isr)
	if err != nil || st != StatusMiss || p.Mode != ModeISR {
		t.Fatalf("ISR must not serve SSG entry: p=%v st=%s err=%v", p, st, err)
	}
}
