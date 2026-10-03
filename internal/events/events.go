// Package events 提供失效事件的投递与处理。
package events

import (
	"sync"
	"sync/atomic"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/cache"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

// Bus 失效事件总线。事件携带 (标签, 版本)，处理是幂等的：
// 重复事件不重复失效，迟到事件不复活已清除内容（判定在缓存侧按
// 版本向量完成，因此乱序到达也不影响最终一致性）。
type Bus struct {
	cache *cache.Cache

	mu     sync.Mutex
	seen   map[string]struct{}
	closed bool

	async bool
	ch    chan asyncEvent
	wg    sync.WaitGroup

	// 观测计数
	received   atomic.Int64
	processed  atomic.Int64
	duplicates atomic.Int64
	invalidated atomic.Int64
}

type asyncEvent struct {
	ev   model.InvalidationEvent
	done chan struct{}
}

// Option 总线配置项。
type Option func(*Bus)

// WithAsync 启用异步投递：事件进入有界队列，由单个 worker 串行处理。
func WithAsync(queueSize int) Option {
	return func(b *Bus) {
		b.async = true
		b.ch = make(chan asyncEvent, queueSize)
	}
}

// New 创建事件总线。
func New(c *cache.Cache, opts ...Option) *Bus {
	b := &Bus{
		cache: c,
		seen:  make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.async {
		b.wg.Add(1)
		go b.worker()
	}
	return b
}

func (b *Bus) worker() {
	defer b.wg.Done()
	for ae := range b.ch {
		if ae.done != nil {
			// 屏障事件：不对应真实失效，仅用于 Flush 等待。
			close(ae.done)
			continue
		}
		b.process(ae.ev)
	}
}

func (b *Bus) process(ev model.InvalidationEvent) {
	n := b.cache.Invalidate(ev.Tag, ev.Version)
	b.processed.Add(1)
	b.invalidated.Add(int64(n))
}

// Publish 投递一个失效事件。同步模式下立即处理；异步模式下入队，
// 由 worker 按到达顺序处理（版本向量使乱序到达同样安全）。
// 重复 EventID 的事件只处理一次。
func (b *Bus) Publish(ev model.InvalidationEvent) {
	b.received.Add(1)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	if _, ok := b.seen[ev.EventID]; ok {
		b.duplicates.Add(1)
		b.mu.Unlock()
		return
	}
	b.seen[ev.EventID] = struct{}{}
	async := b.async
	b.mu.Unlock()

	if async {
		b.ch <- asyncEvent{ev: ev}
		return
	}
	b.process(ev)
}

// Flush 等待队列中所有已投递事件处理完毕（异步模式用于确定性行为）。
func (b *Bus) Flush() {
	if !b.async {
		return
	}
	done := make(chan struct{})
	b.ch <- asyncEvent{done: done}
	<-done
}

// Stats 返回观测计数：收到数、处理数、重复数、累计失效条目数。
func (b *Bus) Stats() (received, processed, duplicates, invalidated int64) {
	return b.received.Load(), b.processed.Load(), b.duplicates.Load(), b.invalidated.Load()
}

// Close 关闭总线；异步模式下等待 worker 退出。
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()
	if b.async {
		close(b.ch)
		b.wg.Wait()
	}
	return nil
}
