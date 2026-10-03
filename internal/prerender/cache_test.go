package prerender

import (
	"context"
	"testing"
	"time"
)

func fillISR(t *testing.T, c *Cache, cfg RouteConfig) *Page {
	t.Helper()
	p, st, err := c.Get(context.Background(), cfg)
	if err != nil || st != StatusMiss {
		t.Fatalf("fill %s: st=%s err=%v", cfg.Path, st, err)
	}
	return p
}

func mustHit(t *testing.T, c *Cache, cfg RouteConfig) {
	t.Helper()
	if _, st, err := c.Get(context.Background(), cfg); err != nil || st != StatusHit {
		t.Fatalf("expected HIT for %s, got st=%s err=%v", cfg.Path, st, err)
	}
}

// 按标签定向失效：一次内容变更只让受影响页面退出缓存。
func TestTargetedInvalidationByTag(t *testing.T) {
	cache, _, _ := newFixture()
	news1 := RouteConfig{Path: "news-1", Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"news"}}
	news2 := RouteConfig{Path: "news-2", Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"news", "front"}}
	sport := RouteConfig{Path: "sport-1", Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"sports"}}

	fillISR(t, cache, news1)
	fillISR(t, cache, news2)
	fillISR(t, cache, sport)
	mustHit(t, cache, news1)
	mustHit(t, cache, news2)
	mustHit(t, cache, sport)

	// 失效 news 标签：仅 news-* 退出缓存，sports 继续命中。
	cache.InvalidateTag("news", 100)
	if _, st, _ := cache.Get(context.Background(), news1); st != StatusMiss {
		t.Fatalf("news-1 should be evicted, got %s", st)
	}
	if _, st, _ := cache.Get(context.Background(), news2); st != StatusMiss {
		t.Fatalf("news-2 should be evicted, got %s", st)
	}
	mustHit(t, cache, sport)
}

// 失效按版本生效：版本高于失效水位的条目不受影响。
func TestInvalidationIsVersionScoped(t *testing.T) {
	cache, _, _ := newFixture()
	cfg := RouteConfig{Path: "p", Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"t"}}

	p := fillISR(t, cache, cfg)           // v1
	cache.InvalidateTag("t", p.Version-1) // 水位低于缓存版本，不应失效
	mustHit(t, cache, cfg)

	cache.InvalidateTag("t", p.Version) // 水位覆盖缓存版本
	if _, st, _ := cache.Get(context.Background(), cfg); st != StatusMiss {
		t.Fatalf("version-covered entry should be evicted, got %s", st)
	}
}

// 已失效的旧版本不得重新进入缓存（Fill 拒绝 + 并发再生的旧写被丢弃）。
func TestFillRejectsInvalidatedVersions(t *testing.T) {
	cache, _, _ := newFixture()
	cfg := RouteConfig{Path: "q", Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"t"}}

	cache.InvalidateTag("t", 5)
	stale := &Page{Route: "q", Mode: ModeISR, Version: 3, Body: []byte("old"), Tags: []string{"t"}}
	if err := cache.Fill(stale, cfg); err == nil {
		t.Fatal("Fill of invalidated version must be rejected")
	}
	if _, ok := cache.lookup("q"); ok {
		t.Fatal("rejected fill must not appear in index")
	}
	if _, ok := cache.backend.Get("q"); ok {
		t.Fatal("rejected fill must not remain in backend")
	}

	// 更高版本可以正常写入。
	fresh := &Page{Route: "q", Mode: ModeISR, Version: 6, Body: []byte("new"), Tags: []string{"t"}}
	if err := cache.Fill(fresh, cfg); err != nil {
		t.Fatalf("Fill of newer version: %v", err)
	}
	mustHit(t, cache, cfg)
}

// 内容存储的原子发布：读取方只能看到完整版本，版本只升不降。
func TestContentStoreAtomicPublish(t *testing.T) {
	store := NewContentStore()
	store.Commit(&Page{Route: "r", Version: 2, Body: []byte("v2")})
	store.Commit(&Page{Route: "r", Version: 1, Body: []byte("v1-late")}) // 迟到旧版本
	p, ok := store.Committed("r")
	if !ok || p.Version != 2 || string(p.Body) != "v2" {
		t.Fatalf("late commit must not roll back: %+v", p)
	}
	if got := store.Version("r"); got != 2 {
		t.Fatalf("version: %d", got)
	}
}
