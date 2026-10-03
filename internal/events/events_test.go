package events

import (
	"testing"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/cache"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

func newTestCache() *cache.Cache {
	return cache.New()
}

func TestBus_SyncPublish(t *testing.T) {
	c := newTestCache()
	b := New(c)
	defer b.Close()

	b.Publish(model.InvalidationEvent{Tag: "t1", Version: 1, EventID: "e1"})
	received, processed, dups, _ := b.Stats()
	if received != 1 || processed != 1 || dups != 0 {
		t.Fatalf("want 1/1/0, got %d/%d/%d", received, processed, dups)
	}
}

func TestBus_DuplicateDedup(t *testing.T) {
	c := newTestCache()
	b := New(c)
	defer b.Close()

	ev := model.InvalidationEvent{Tag: "t1", Version: 1, EventID: "e1"}
	b.Publish(ev)
	b.Publish(ev) // 重复
	b.Publish(ev) // 重复
	received, processed, dups, _ := b.Stats()
	if received != 3 || processed != 1 || dups != 2 {
		t.Fatalf("want 3/1/2, got %d/%d/%d", received, processed, dups)
	}
}

func TestBus_AsyncFlush(t *testing.T) {
	c := newTestCache()
	b := New(c, WithAsync(16))
	defer b.Close()

	for i := 0; i < 10; i++ {
		b.Publish(model.InvalidationEvent{
			Tag:     "t1",
			Version: int64(i + 1),
			EventID: "e" + string(rune('a'+i)),
		})
	}
	b.Flush()
	received, processed, _, _ := b.Stats()
	if received != 10 || processed != 10 {
		t.Fatalf("want 10/10, got %d/%d", received, processed)
	}
}

func TestBus_AsyncDuplicate(t *testing.T) {
	c := newTestCache()
	b := New(c, WithAsync(16))
	defer b.Close()

	ev := model.InvalidationEvent{Tag: "t1", Version: 1, EventID: "e1"}
	b.Publish(ev)
	b.Publish(ev)
	b.Flush()
	received, processed, dups, _ := b.Stats()
	if received != 2 || processed != 1 || dups != 1 {
		t.Fatalf("want 2/1/1, got %d/%d/%d", received, processed, dups)
	}
}

func TestBus_CloseIdempotent(t *testing.T) {
	c := newTestCache()
	b := New(c)
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	// 关闭后投递不处理
	b.Publish(model.InvalidationEvent{Tag: "t1", Version: 1, EventID: "e1"})
	_, processed, _, _ := b.Stats()
	if processed != 0 {
		t.Fatalf("closed bus must not process, got %d", processed)
	}
}

func TestBus_InvalidationCount(t *testing.T) {
	c := newTestCache()
	// 放入两个条目，依赖 t1 版本 1
	route := model.Route{Path: "/a", Mode: model.ModeISR, Tags: []string{"t1"}, FreshTTL: time.Minute, StaleWindow: time.Minute}
	c.Put(cache.NewEntry(route, &model.Content{Route: "/a", Mode: model.ModeISR, Version: 1, Deps: model.VersionVector{"t1": 1}}, time.Now()))
	c.Put(cache.NewEntry(route, &model.Content{Route: "/b", Mode: model.ModeISR, Version: 1, Deps: model.VersionVector{"t1": 1}}, time.Now()))

	b := New(c)
	defer b.Close()
	b.Publish(model.InvalidationEvent{Tag: "t1", Version: 2, EventID: "e1"})
	_, _, _, invalidated := b.Stats()
	if invalidated != 2 {
		t.Fatalf("want 2 invalidated, got %d", invalidated)
	}
}
