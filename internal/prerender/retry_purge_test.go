package prerender

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// faultyBackend 可注入瞬时故障的 Backend 包装。
type faultyBackend struct {
	inner      *MemoryBackend
	failSet    atomic.Bool
	failDelete atomic.Bool
	setCalls   atomic.Int64
}

func newFaultyBackend() *faultyBackend { return &faultyBackend{inner: NewMemoryBackend()} }

func (f *faultyBackend) Get(key string) (*Page, bool) { return f.inner.Get(key) }
func (f *faultyBackend) Keys() []string               { return f.inner.Keys() }

func (f *faultyBackend) Set(key string, p *Page) error {
	f.setCalls.Add(1)
	if f.failSet.Load() {
		return errors.New("backend set boom")
	}
	return f.inner.Set(key, p)
}

func (f *faultyBackend) Delete(key string) error {
	if f.failDelete.Load() {
		return errors.New("backend delete boom")
	}
	return f.inner.Delete(key)
}

func TestDoWithRetryBoundedBackoff(t *testing.T) {
	// 前两次失败、第三次成功。
	var calls int64
	err := DoWithRetry(context.Background(), RetryConfig{MaxAttempts: 3}, func() error {
		if atomic.AddInt64(&calls, 1) < 3 {
			return errors.New("boom")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}

	// 反复失败：次数有界，返回最后一次错误。
	calls = 0
	err = DoWithRetry(context.Background(), RetryConfig{MaxAttempts: 4}, func() error {
		atomic.AddInt64(&calls, 1)
		return errors.New("always")
	})
	if err == nil || calls != 4 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}

	// 退避有界：MaxDelay 限制单次等待。
	start := time.Now()
	_ = DoWithRetry(context.Background(), RetryConfig{MaxAttempts: 4, BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Millisecond}, func() error {
		return errors.New("always")
	})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("backoff not bounded: %v", elapsed)
	}
}

// flakyOrigin 前 failTimes 次调用失败，之后成功（确定性瞬时故障）。
type flakyOrigin struct {
	calls     atomic.Int64
	failTimes int64
}

func (f *flakyOrigin) Render(_ context.Context, route string, version uint64) (*Page, error) {
	if f.calls.Add(1) <= f.failTimes {
		return nil, errors.New("transient boom")
	}
	return &Page{Body: []byte("ok")}, nil
}

// 回源瞬时失败：渲染器按有界退避重试后成功。
func TestRendererRetriesTransientOriginFailure(t *testing.T) {
	origin := &flakyOrigin{failTimes: 2}
	retry := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond}
	r := NewRenderer(origin, retry)
	cfg := isrCfg("retry")

	p, err := r.Render(context.Background(), cfg, 1)
	if err != nil {
		t.Fatalf("render with retry: %v", err)
	}
	if p.Mode != ModeISR || p.Route != "retry" || p.Version != 1 {
		t.Fatalf("page not stamped by route config: %+v", p)
	}
	if got := origin.calls.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

// 回写反复失败：不得损坏缓存索引（索引不出现半成品条目）。
func TestFillFailureKeepsIndexClean(t *testing.T) {
	fb := newFaultyBackend()
	fb.failSet.Store(true)
	retry := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond}
	store := NewContentStore()
	cache := NewCache(fb, store, NewRenderer(&fakeOrigin{}, retry), retry)

	cfg := isrCfg("fragile")
	p := &Page{Route: "fragile", Mode: ModeISR, Version: 1, Body: []byte("x"), Tags: cfg.Tags}
	if err := cache.Fill(p, cfg); err == nil {
		t.Fatal("fill should fail")
	}
	if _, ok := cache.lookup("fragile"); ok {
		t.Fatal("index must not contain entry after failed fill")
	}
	if got := fb.setCalls.Load(); got != 3 {
		t.Fatalf("expected 3 bounded attempts, got %d", got)
	}
}

// 删除反复失败：索引先移除（不可再读），后端 key 进入待清理队列，
// 故障恢复后 SweepOrphans 清除，不留无法读取也无法清除的孤儿。
func TestDeleteFailureLeavesNoOrphan(t *testing.T) {
	fb := newFaultyBackend()
	retry := RetryConfig{MaxAttempts: 2, BaseDelay: time.Millisecond}
	store := NewContentStore()
	cache := NewCache(fb, store, NewRenderer(&fakeOrigin{}, retry), retry)

	cfg := isrCfg("doomed")
	fillISR(t, cache, cfg)
	mustHit(t, cache, cfg)

	fb.failDelete.Store(true)
	cache.InvalidateTag("t-doomed", 100)

	// 索引已移除：不再可读。
	if _, ok := cache.lookup("doomed"); ok {
		t.Fatal("index entry must be removed even when backend delete fails")
	}
	if cache.PendingDeletes() != 1 {
		t.Fatalf("expected 1 pending delete, got %d", cache.PendingDeletes())
	}

	// 故障恢复后清扫：孤儿被清除。
	fb.failDelete.Store(false)
	cache.SweepOrphans()
	if cache.PendingDeletes() != 0 {
		t.Fatalf("pending deletes not drained: %d", cache.PendingDeletes())
	}
	if len(fb.Keys()) != 0 {
		t.Fatalf("orphan keys remain: %v", fb.Keys())
	}
}

// 乱序、重复、迟到的失效事件：最终状态与按序全量处理一致。
func TestPurgeEventsOutOfOrderDuplicateLate(t *testing.T) {
	build := func(order []PurgeEvent) (*Cache, *PurgeLog) {
		cache, _, _ := newFixture()
		log := NewPurgeLog()
		// 10 条路由各携带一个版本（1..10），均带标签 t。
		for v := uint64(1); v <= 10; v++ {
			route := "e" + itoa(v)
			cfg := RouteConfig{Path: route, Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"t"}}
			p := &Page{Route: route, Mode: ModeISR, Version: v, Body: []byte("x"), Tags: cfg.Tags}
			if err := cache.Fill(p, cfg); err != nil {
				t.Fatalf("fill v%d: %v", v, err)
			}
		}
		for _, ev := range order {
			log.Apply(cache, ev)
		}
		return cache, log
	}

	inOrder := []PurgeEvent{
		{Seq: 1, Tag: "t", Version: 4},
		{Seq: 2, Tag: "t", Version: 7},
		{Seq: 3, Tag: "t", Version: 9},
	}
	// 同一批事件的乱序 + 重复 + 迟到（低版本）混合。
	shuffled := []PurgeEvent{
		{Seq: 2, Tag: "t", Version: 7},
		{Seq: 1, Tag: "t", Version: 4}, // 乱序
		{Seq: 2, Tag: "t", Version: 7}, // 重复
		{Seq: 3, Tag: "t", Version: 9},
		{Seq: 4, Tag: "t", Version: 2}, // 迟到的旧事件
	}

	cacheA, logA := build(inOrder)
	cacheB, logB := build(shuffled)

	if logB.Watermark("t") != 9 || logA.Watermark("t") != logB.Watermark("t") {
		t.Fatalf("watermarks: in-order=%d shuffled=%d", logA.Watermark("t"), logB.Watermark("t"))
	}
	// 最终缓存状态一致：版本 <= 9 的条目都被清除，v10 保留。
	for _, c := range []*Cache{cacheA, cacheB} {
		for v := uint64(1); v <= 9; v++ {
			if _, ok := c.lookup("e" + itoa(v)); ok {
				t.Fatalf("entry v%d (<= watermark) must be evicted", v)
			}
		}
		if _, ok := c.lookup("e10"); !ok {
			t.Fatal("entry v10 (> watermark) must survive")
		}
	}

	// 水位之上的新写入不受迟到事件影响（迟到事件不得复活已清除页面，
	// 也不得误杀新版本）。
	cfg := RouteConfig{Path: "e10", Mode: ModeISR, FreshTTL: time.Hour, Tags: []string{"t"}}
	logB.Apply(cacheB, PurgeEvent{Seq: 5, Tag: "t", Version: 3}) // 迟到旧事件
	mustHit(t, cacheB, cfg)
	if _, ok := cacheB.lookup("e10"); !ok {
		t.Fatal("late event must not evict newer content")
	}
}

// 全量重放验证：随机乱序应用事件后，缓存状态与按序处理一致。
func TestPurgeEventualConsistencyWithShuffle(t *testing.T) {
	events := []PurgeEvent{
		{Seq: 1, Tag: "a", Version: 3},
		{Seq: 2, Tag: "b", Version: 5},
		{Seq: 3, Tag: "a", Version: 8},
		{Seq: 4, Tag: "b", Version: 2},
		{Seq: 5, Tag: "a", Version: 6},
	}
	apply := func(evs []PurgeEvent) map[string]uint64 {
		l := NewPurgeLog()
		c := NewCache(NewMemoryBackend(), NewContentStore(), NewRenderer(&fakeOrigin{}, RetryConfig{MaxAttempts: 1}), RetryConfig{MaxAttempts: 1})
		for _, ev := range evs {
			l.Apply(c, ev)
		}
		return map[string]uint64{"a": l.Watermark("a"), "b": l.Watermark("b")}
	}
	want := apply(events)
	for trial := 0; trial < 20; trial++ {
		shuffled := append([]PurgeEvent(nil), events...)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		if got := apply(shuffled); got["a"] != want["a"] || got["b"] != want["b"] {
			t.Fatalf("trial %d: got %v want %v", trial, got, want)
		}
	}
	if want["a"] != 8 || want["b"] != 5 {
		t.Fatalf("watermarks should be max version per tag: %v", want)
	}
}
