package cache

import (
	"testing"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

func testRoute(path string, mode model.RenderMode, tags []string, fresh, stale time.Duration) model.Route {
	return model.Route{
		Path:        path,
		Mode:        mode,
		Tags:        tags,
		FreshTTL:    fresh,
		StaleWindow: stale,
	}
}

func testContent(route model.Route, version int64, deps model.VersionVector) *model.Content {
	return &model.Content{
		Route:     route.Path,
		Mode:      route.Mode,
		Body:      []byte("body-" + route.Path),
		MediaType: "text/html",
		Version:   version,
		Deps:      deps,
	}
}

func TestEntry_StateTransitions(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	c := New(WithClock(clock))

	route := testRoute("/a", model.ModeISR, []string{"t1"}, 10*time.Second, 20*time.Second)
	e := NewEntry(route, testContent(route, 1, model.VersionVector{"t1": 1}), now)
	c.Put(e)

	// 新鲜期内
	got, _, ok := c.Get("/a")
	if !ok || got.Refresh(now) != StateFresh {
		t.Fatalf("want fresh, got ok=%v state=%v", ok, got.State())
	}

	// 进入陈旧窗口
	now = now.Add(10 * time.Second)
	got, _, _ = c.Get("/a")
	if got.Refresh(now) != StateStale {
		t.Fatalf("want stale, got %v", got.State())
	}

	// 超出陈旧窗口
	now = now.Add(20 * time.Second)
	got, _, _ = c.Get("/a")
	if got.Refresh(now) != StateExpired {
		t.Fatalf("want expired, got %v", got.State())
	}
}

func TestInvalidate_TargetedTag(t *testing.T) {
	now := time.Unix(1000, 0)
	c := New(WithClock(func() time.Time { return now }))

	rA := testRoute("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute)
	rB := testRoute("/b", model.ModeISR, []string{"t2"}, time.Minute, time.Minute)
	rC := testRoute("/c", model.ModeISR, []string{"t1", "t2"}, time.Minute, time.Minute)
	c.Put(NewEntry(rA, testContent(rA, 1, model.VersionVector{"t1": 1}), now))
	c.Put(NewEntry(rB, testContent(rB, 2, model.VersionVector{"t2": 1}), now))
	c.Put(NewEntry(rC, testContent(rC, 3, model.VersionVector{"t1": 1, "t2": 1}), now))

	// t1 变更到版本 2：仅失效依赖 t1 且版本 <2 的条目（/a 与 /c），/b 保持命中
	n := c.Invalidate("t1", 2)
	if n != 2 {
		t.Fatalf("want 2 invalidated, got %d", n)
	}
	for _, p := range []string{"/a", "/c"} {
		e, _, _ := c.Get(p)
		if e.State() != StateInvalidated {
			t.Errorf("%s want invalidated, got %v", p, e.State())
		}
	}
	if e, _, _ := c.Get("/b"); e.State() != StateFresh {
		t.Errorf("/b want fresh (other tag unaffected), got %v", e.State())
	}
}

func TestInvalidate_IdempotentAndLateEvents(t *testing.T) {
	now := time.Unix(1000, 0)
	c := New(WithClock(func() time.Time { return now }))

	r := testRoute("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute)
	c.Put(NewEntry(r, testContent(r, 1, model.VersionVector{"t1": 1}), now))

	// 事件 (t1, 2) 失效条目
	if n := c.Invalidate("t1", 2); n != 1 {
		t.Fatalf("want 1, got %d", n)
	}
	// 重复事件：幂等，不再失效任何东西
	if n := c.Invalidate("t1", 2); n != 0 {
		t.Fatalf("duplicate event: want 0, got %d", n)
	}
	// 迟到的旧事件 (t1, 1)：不得复活已清除内容
	if n := c.Invalidate("t1", 1); n != 0 {
		t.Fatalf("late event: want 0 (no resurrection), got %d", n)
	}
	if e, _, _ := c.Get("/a"); e.State() != StateInvalidated {
		t.Fatalf("want stays invalidated, got %v", e.State())
	}

	// 新版本内容发布后，迟到事件不得误伤
	c.Put(NewEntry(r, testContent(r, 2, model.VersionVector{"t1": 2}), now))
	if n := c.Invalidate("t1", 1); n != 0 {
		t.Fatalf("late event must not hit newer content, got %d", n)
	}
	if e, _, _ := c.Get("/a"); e.State() != StateFresh {
		t.Fatalf("new content must survive late event, got %v", e.State())
	}
	// 而真正的新事件 (t1, 2) 对新版本也幂等（不失效）
	if n := c.Invalidate("t1", 2); n != 0 {
		t.Fatalf("event at same version must not invalidate, got %d", n)
	}
}

func TestPut_AtomicWholeEntryReplace(t *testing.T) {
	now := time.Unix(1000, 0)
	c := New(WithClock(func() time.Time { return now }))

	r := testRoute("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute)
	c1 := testContent(r, 1, model.VersionVector{"t1": 1})
	c2 := testContent(r, 2, model.VersionVector{"t1": 2})
	c.Put(NewEntry(r, c1, now))
	c.Put(NewEntry(r, c2, now))

	e, _, ok := c.Get("/a")
	if !ok {
		t.Fatal("entry missing")
	}
	if e.Content().Version != 2 || string(e.Content().Body) != "body-/a" {
		t.Fatalf("entry should be whole new version, got version=%d body=%q", e.Content().Version, e.Content().Body)
	}
	if e.Deps()["t1"] != 2 {
		t.Fatalf("deps should be fully replaced, got %v", e.Deps())
	}
}

func TestEntry_ModeIsCarried(t *testing.T) {
	now := time.Unix(1000, 0)
	c := New(WithClock(func() time.Time { return now }))

	for _, mode := range []model.RenderMode{model.ModeSSG, model.ModeSSR, model.ModeISR} {
		r := testRoute("/"+string(mode), mode, nil, time.Minute, time.Minute)
		c.Put(NewEntry(r, testContent(r, 1, nil), now))
		e, _, ok := c.Get("/" + string(mode))
		if !ok || e.Mode() != mode {
			t.Fatalf("mode %s not carried, got ok=%v mode=%v", mode, ok, e.Mode())
		}
	}
}

func TestInvalidate_DoesNotReviveExpiredOrStale(t *testing.T) {
	now := time.Unix(1000, 0)
	c := New(WithClock(func() time.Time { return now }))

	r := testRoute("/a", model.ModeISR, []string{"t1"}, time.Minute, time.Minute)
	c.Put(NewEntry(r, testContent(r, 1, model.VersionVector{"t1": 1}), now))

	// 时间推进到陈旧窗口后再失效：条目应保持 invalidated 而非被“刷新”成 stale
	now = now.Add(2 * time.Minute)
	c.Invalidate("t1", 2)
	e, _, _ := c.Get("/a")
	if e.State() != StateInvalidated {
		t.Fatalf("want invalidated, got %v", e.State())
	}
}
