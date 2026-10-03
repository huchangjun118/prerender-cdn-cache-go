// Package cache 提供带标签版本向量、新鲜度状态机与陈旧窗口的边缘缓存。
package cache

import (
	"sync"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

// State 缓存条目状态。
type State int

const (
	// StateFresh 新鲜：可直接命中返回。
	StateFresh State = iota
	// StateStale 陈旧但仍在陈旧窗口内：返回陈旧内容并后台再生。
	StateStale
	// StateExpired 超出陈旧窗口：必须同步再生。
	StateExpired
	// StateInvalidated 已被失效事件清除：不得返回。
	StateInvalidated
)

// String 输出状态名。
func (s State) String() string {
	switch s {
	case StateFresh:
		return "fresh"
	case StateStale:
		return "stale"
	case StateExpired:
		return "expired"
	case StateInvalidated:
		return "invalidated"
	}
	return "unknown"
}

// Entry 是一个缓存条目。条目持有的内容不可变，状态变更在缓存锁内完成。
type Entry struct {
	content    *model.Content
	mode       model.RenderMode
	deps       model.VersionVector
	state      State
	createdAt  time.Time
	freshUntil time.Time
	staleUntil time.Time
}

// NewEntry 依据路由声明与内容创建条目，新鲜度边界在此时一次性算定，
// 之后不随发布变化（条目是某次渲染的不可变快照）。
func NewEntry(route model.Route, content *model.Content, now time.Time) *Entry {
	freshUntil := now.Add(route.FreshTTL)
	staleUntil := freshUntil.Add(route.StaleWindow)
	deps := make(model.VersionVector, len(content.Deps))
	for k, v := range content.Deps {
		deps[k] = v
	}
	return &Entry{
		content:    content,
		mode:       route.Mode,
		deps:       deps,
		state:      StateFresh,
		createdAt:  now,
		freshUntil: freshUntil,
		staleUntil: staleUntil,
	}
}

// Content 返回条目持有的不可变内容。
func (e *Entry) Content() *model.Content { return e.content }

// State 返回条目最近一次重算的状态（新鲜度由 Get/Entries 按时钟重算）。
func (e *Entry) State() State { return e.state }

// Mode 返回条目内容的产出模式。
func (e *Entry) Mode() model.RenderMode { return e.mode }

// Deps 返回条目依赖的标签版本向量副本。
func (e *Entry) Deps() model.VersionVector {
	out := make(model.VersionVector, len(e.deps))
	for k, v := range e.deps {
		out[k] = v
	}
	return out
}

// CreatedAt 返回条目创建时间。
func (e *Entry) CreatedAt() time.Time { return e.createdAt }

// FreshUntil 返回新鲜期截止时刻。
func (e *Entry) FreshUntil() time.Time { return e.freshUntil }

// StaleUntil 返回陈旧窗口截止时刻。
func (e *Entry) StaleUntil() time.Time { return e.staleUntil }

// Refresh 依据当前时钟重算并返回条目状态。已失效条目保持失效，
// 不会因时间流逝或后续事件而复活。
func (e *Entry) Refresh(now time.Time) State {
	if e.state == StateInvalidated {
		return StateInvalidated
	}
	switch {
	case now.Before(e.freshUntil):
		e.state = StateFresh
	case now.Before(e.staleUntil):
		e.state = StateStale
	default:
		e.state = StateExpired
	}
	return e.state
}

// Cache 边缘缓存。所有索引变更都在同一把锁内完成，保证并发自洽：
// 失效与发布/写入不会交错出半成品状态。
type Cache struct {
	mu      sync.Mutex
	entries map[string]*Entry
	// tagHighWater 记录每个标签已处理失效事件的最高版本。
	// 新写入条目若某标签依赖版本低于高水位，说明其内容在一次失效之后
	// 才到达（渲染晚于变更），直接标记为失效，保证“已失效旧内容不复活”。
	tagHighWater map[string]int64
	clock        func() time.Time
}

// Option 缓存配置项。
type Option func(*Cache)

// WithClock 注入时钟（测试用，便于快进验证陈旧窗口）。
func WithClock(clock func() time.Time) Option {
	return func(c *Cache) { c.clock = clock }
}

// New 创建缓存。
func New(opts ...Option) *Cache {
	c := &Cache{
		entries:      make(map[string]*Entry),
		tagHighWater: make(map[string]int64),
		clock:        time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get 读取路由条目并在锁内一次性算定状态后返回。条目指针持有的内容
// 不可变，读取方在锁外使用内容是安全的；状态以本次返回为准，
// 避免与并发失效发生数据竞争。
func (c *Cache) Get(route string) (*Entry, State, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[route]
	if !ok {
		return nil, StateExpired, false
	}
	state := e.Refresh(c.clock())
	return e, state, true
}

// Put 原子地写入一个条目：同 key 旧条目被整体替换，不做字段合并，
// 因此不会产生新旧字段混合的条目。若条目依赖的标签版本低于该标签的
// 失效高水位（内容晚于失效到达），条目标记为失效，旧内容不会复活。
func (c *Cache) Put(e *Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for tag, v := range e.deps {
		if hw := c.tagHighWater[tag]; v < hw {
			e.state = StateInvalidated
		}
	}
	c.entries[e.content.Route] = e
}

// Invalidate 按标签与版本定向失效。仅失效依赖该标签且版本严格小于
// version 的条目，其余条目（含其它标签、或依赖版本更新的条目）继续
// 保持命中，不会整站清空。乱序、重复、迟到事件均幂等，且迟到事件
// 不会复活已清除内容。返回受影响条目数。
func (c *Cache) Invalidate(tag string, version int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.clock()
	if version > c.tagHighWater[tag] {
		c.tagHighWater[tag] = version
	}
	count := 0
	for _, e := range c.entries {
		if e.state == StateInvalidated {
			continue
		}
		if dep, ok := e.deps[tag]; ok && dep < version {
			e.state = StateInvalidated
			count++
			continue
		}
		// 即使未被失效，也重算一次状态，保证返回的计数与观测一致。
		e.Refresh(n)
	}
	return count
}

// Delete 移除指定路由的条目（全量重建或清理时使用）。
func (c *Cache) Delete(route string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, route)
}

// Clear 清空全部条目与标签高水位（全量重建时使用）。
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*Entry)
	c.tagHighWater = make(map[string]int64)
}

// Entries 返回当前所有条目的快照（观测/测试/全量重建校验用）。
func (c *Cache) Entries() []*Entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Entry, 0, len(c.entries))
	for _, e := range c.entries {
		e.Refresh(c.clock())
		out = append(out, e)
	}
	return out
}

// TagHighWater 返回某标签的失效高水位（观测用）。
func (c *Cache) TagHighWater(tag string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tagHighWater[tag]
}

// Now 返回缓存当前时钟。
func (c *Cache) Now() time.Time {
	if c.clock == nil {
		return time.Now()
	}
	return c.clock()
}
