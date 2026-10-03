// Package store 提供按内容版本的原子化存储。
package store

import (
	"context"
	"sync"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

// Store 保存每个路由当前已提交的内容版本。内容不可变，发布操作在锁内
// 整体切换当前版本指针，因此任何读取都只能看到某个已提交的完整版本，
// 不会看到半成品或字段级混合。
type Store struct {
	mu      sync.RWMutex
	seq     int64                  // 全局发布序号
	tagSeq  map[string]int64       // 标签 -> 当前版本
	current map[string]*model.Content
	history map[string][]*model.Content
}

// New 创建存储实例。
func New() *Store {
	return &Store{
		tagSeq:  make(map[string]int64),
		current: make(map[string]*model.Content),
		history: make(map[string][]*model.Content),
	}
}

// Publish 原子地提交一个内容版本：分配全局发布序号、推进标签版本向量、
// 整体切换当前版本指针，返回全局发布序号。调用方在锁外完成渲染（I/O），
// 锁内只做指针切换，因此发布期间读取方要么看到完整旧版本，要么看到
// 完整新版本。
func (s *Store) Publish(ctx context.Context, c *model.Content, tags []string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	c.Version = s.seq
	c.CreatedAt = time.Now()

	// 标签版本与内容版本在同一临界区内推进，保证内容的 Deps 向量
	// 恰好等于发布后的标签版本，内容不会被自己的失效事件误伤。
	c.Deps = make(model.VersionVector, len(tags))
	for _, tag := range tags {
		s.tagSeq[tag]++
		c.Deps[tag] = s.tagSeq[tag]
	}

	s.current[c.Route] = c
	s.history[c.Route] = append(s.history[c.Route], c)
	return c.Version, nil
}

// Current 读取某路由当前已提交的内容。返回的是不可变内容指针，
// 读取方无需担心字段在读取过程中被修改。
func (s *Store) Current(route string) (*model.Content, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.current[route]
	return c, ok
}

// Version 读取某路由指定发布序号的历史内容。
func (s *Store) Version(route string, version int64) (*model.Content, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.history[route] {
		if c.Version == version {
			return c, true
		}
	}
	return nil, false
}

// TagVersion 返回某标签的当前版本（未出现过返回 0）。
func (s *Store) TagVersion(tag string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tagSeq[tag]
}

// TagVersions 返回一组标签的当前版本快照。
func (s *Store) TagVersions(tags []string) model.VersionVector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	deps := make(model.VersionVector, len(tags))
	for _, tag := range tags {
		deps[tag] = s.tagSeq[tag]
	}
	return deps
}

// Routes 返回所有已发布内容的路由。
func (s *Store) Routes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	routes := make([]string, 0, len(s.current))
	for r := range s.current {
		routes = append(routes, r)
	}
	return routes
}
