package prerender

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNotPublished 表示路由从未发布过内容，无法提供任何版本。
var ErrNotPublished = errors.New("prerender: route not published")

// entryMeta 是缓存索引中的条目元数据；索引是读取的唯一依据。
type entryMeta struct {
	version    uint64
	mode       RenderMode
	tags       []string
	freshUntil time.Time
	staleUntil time.Time
}

// Cache 是边缘缓存：维护路由到缓存条目的索引，
// 条目带标签以支持定向失效，带新鲜期/陈旧窗口以支持失败兜底。
//
// 一致性不变量：
//   - 索引中登记的条目一定已在后端写入成功（反复失败不会损坏索引）；
//   - 后端删除失败的 key 会进入待清理队列，可由 SweepOrphans 清除，
//     不会留下无法读取也无法清除的孤儿条目；
//   - 版本不高于失效水位的写入会被拒绝，已失效的旧内容不会重新进入缓存。
type Cache struct {
	backend  Backend
	store    *ContentStore
	renderer *Renderer
	retry    RetryConfig
	now      func() time.Time

	mu            sync.Mutex
	index         map[string]*entryMeta // route -> 元数据
	tagWatermark  map[string]uint64     // tag -> 已应用的失效版本水位
	pendingDelete []string              // 后端删除失败待清理的 key
}

// NewCache 创建缓存。
func NewCache(b Backend, store *ContentStore, renderer *Renderer, retry RetryConfig) *Cache {
	return &Cache{
		backend:      b,
		store:        store,
		renderer:     renderer,
		retry:        retry,
		now:          time.Now,
		index:        make(map[string]*entryMeta),
		tagWatermark: make(map[string]uint64),
	}
}

// Get 读取路由内容，行为由路由声明的渲染模式决定。
func (c *Cache) Get(ctx context.Context, cfg RouteConfig) (*Page, CacheStatus, error) {
	switch cfg.Mode {
	case ModeSSG:
		return c.getSSG(cfg)
	case ModeSSR:
		return c.getSSR(ctx, cfg)
	case ModeISR:
		return c.getISR(ctx, cfg)
	default:
		return nil, StatusError, fmt.Errorf("prerender: unknown render mode %q", cfg.Mode)
	}
}

// getSSG 构建期预生成：运行时不回源，只读缓存与已提交版本。
func (c *Cache) getSSG(cfg RouteConfig) (*Page, CacheStatus, error) {
	committed, ok := c.store.Committed(cfg.Path)
	if !ok {
		return nil, StatusError, ErrNotPublished
	}
	if meta, hit := c.lookup(cfg.Path); hit &&
		meta.mode == ModeSSG && meta.version >= committed.Version &&
		c.now().Before(meta.freshUntil) {
		if p, ok := c.backend.Get(cfg.Path); ok {
			return p, StatusHit, nil
		}
	}
	// 已提交版本已被失效事件覆盖：在新发布之前没有可服务的有效版本。
	if committed.Version <= c.tagWatermarkMax(committed.Tags) {
		return nil, StatusError, ErrNotPublished
	}
	// 未命中或缓存落后于已提交版本：用已提交的完整版本回填。
	if err := c.Fill(committed, cfg); err != nil {
		return nil, StatusError, err
	}
	return committed, StatusMiss, nil
}

// getSSR 请求时实时渲染：每次回源，不写缓存；
// 失败时按策略回退到最近已提交版本（陈旧）或直接报错。
func (c *Cache) getSSR(ctx context.Context, cfg RouteConfig) (*Page, CacheStatus, error) {
	version := c.store.Version(cfg.Path) + 1
	p, err := c.renderer.Render(ctx, cfg, version)
	if err == nil {
		return p, StatusMiss, nil
	}
	if cfg.StalePolicy == StalePolicyServeStale {
		if committed, ok := c.store.Committed(cfg.Path); ok {
			return committed, StatusStale, nil
		}
	}
	return nil, StatusError, err
}

// getISR 增量再生：新鲜命中直接返回；过期或缺失则再生并回写；
// 再生失败时按策略在陈旧内容与错误之间选择。
func (c *Cache) getISR(ctx context.Context, cfg RouteConfig) (*Page, CacheStatus, error) {
	meta, hit := c.lookup(cfg.Path)
	now := c.now()
	if hit && meta.mode == ModeISR && now.Before(meta.freshUntil) {
		if p, ok := c.backend.Get(cfg.Path); ok {
			return p, StatusHit, nil
		}
	}
	var cachedVersion uint64
	if hit {
		cachedVersion = meta.version
	}
	// 再生版本：优先采用已提交的内容版本；同时必须高于缓存版本与
	// 失效水位——水位语义是"版本 <= 水位即失效"，新内容必须高于水位。
	version := c.store.Version(cfg.Path)
	if version <= cachedVersion {
		version = cachedVersion + 1
	}
	if wm := c.tagWatermarkMax(cfg.Tags); version <= wm {
		version = wm + 1
	}
	p, err := c.renderer.Render(ctx, cfg, version)
	if err != nil {
		// 再生失败：处于陈旧窗口内且策略允许时返回陈旧内容。
		if hit && cfg.StalePolicy == StalePolicyServeStale && now.Before(meta.staleUntil) {
			if stale, ok := c.backend.Get(cfg.Path); ok {
				return stale, StatusStale, nil
			}
		}
		return nil, StatusError, err
	}
	if err := c.Fill(p, cfg); err != nil {
		return nil, StatusError, err
	}
	return p, StatusMiss, nil
}

// Fill 将页面写入缓存。版本不高于该页面任一标签失效水位的写入会被拒绝，
// 防止已失效的旧内容在并发下重新进入缓存。
func (c *Cache) Fill(p *Page, cfg RouteConfig) error {
	// 先写后端（带重试），成功后才登记索引：反复失败不会损坏索引。
	err := DoWithRetry(context.Background(), c.retry, func() error {
		return c.backend.Set(p.Route, p)
	})
	if err != nil {
		return fmt.Errorf("prerender: cache fill: %w", err)
	}
	now := c.now()
	freshUntil := now.Add(cfg.FreshTTL)
	if cfg.FreshTTL <= 0 {
		freshUntil = now.Add(100 * 365 * 24 * time.Hour) // 未配置新鲜期视为长期有效
	}
	staleUntil := freshUntil.Add(cfg.StaleTTL)

	c.mu.Lock()
	defer c.mu.Unlock()
	if p.Version <= c.maxWatermarkLocked(p.Tags) {
		// 写入期间发生了覆盖该版本的失效：立即撤除，不留孤儿。
		c.mu.Unlock()
		_ = DoWithRetry(context.Background(), c.retry, func() error {
			return c.backend.Delete(p.Route)
		})
		c.mu.Lock()
		return fmt.Errorf("prerender: fill rejected, version %d already invalidated", p.Version)
	}
	if cur, ok := c.index[p.Route]; ok && cur.version > p.Version {
		return nil // 已有更新版本，丢弃迟到的旧写入
	}
	c.index[p.Route] = &entryMeta{
		version:    p.Version,
		mode:       p.Mode,
		tags:       append([]string(nil), p.Tags...),
		freshUntil: freshUntil,
		staleUntil: staleUntil,
	}
	return nil
}

// InvalidateTag 按标签定向失效：仅移除携带该标签且版本不高于
// 失效版本的条目，其余页面继续保持命中。版本水位只升不降，
// 因此乱序、重复、迟到的事件都是安全的。
func (c *Cache) InvalidateTag(tag string, version uint64) {
	c.mu.Lock()
	if version > c.tagWatermark[tag] {
		c.tagWatermark[tag] = version
	}
	var doomed []string
	for route, meta := range c.index {
		if meta.version > version || !hasTag(meta.tags, tag) {
			continue
		}
		doomed = append(doomed, route)
		delete(c.index, route)
	}
	c.mu.Unlock()

	for _, route := range doomed {
		c.deleteBackend(route)
	}
}

// SweepOrphans 重试待清理的后端删除，并移除后端中未在索引登记的 key，
// 保证反复失败后也不会留下无法读取也无法清除的孤儿条目。
func (c *Cache) SweepOrphans() {
	c.mu.Lock()
	pending := append([]string(nil), c.pendingDelete...)
	c.pendingDelete = c.pendingDelete[:0]
	c.mu.Unlock()

	for _, key := range pending {
		c.deleteBackend(key)
	}

	c.mu.Lock()
	indexed := make(map[string]struct{}, len(c.index))
	for route := range c.index {
		indexed[route] = struct{}{}
	}
	c.mu.Unlock()
	for _, key := range c.backend.Keys() {
		if _, ok := indexed[key]; !ok {
			c.deleteBackend(key)
		}
	}
}

// PendingDeletes 返回待清理的后端 key 数（用于观测与测试）。
func (c *Cache) PendingDeletes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pendingDelete)
}

func (c *Cache) lookup(route string) (*entryMeta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	meta, ok := c.index[route]
	if !ok {
		return nil, false
	}
	cp := *meta
	return &cp, true
}

// tagWatermarkMax 返回这些标签中最高的失效版本水位。
func (c *Cache) tagWatermarkMax(tags []string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxWatermarkLocked(tags)
}

func (c *Cache) maxWatermarkLocked(tags []string) uint64 {
	var max uint64
	for _, t := range tags {
		if w := c.tagWatermark[t]; w > max {
			max = w
		}
	}
	return max
}

func (c *Cache) deleteBackend(key string) {
	err := DoWithRetry(context.Background(), c.retry, func() error {
		return c.backend.Delete(key)
	})
	if err != nil {
		// 索引已移除（不可再读取），key 进入待清理队列等待 SweepOrphans。
		c.mu.Lock()
		c.pendingDelete = append(c.pendingDelete, key)
		c.mu.Unlock()
	}
}

func hasTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}
