// Package renderer 提供渲染能力与可失败的下游数据源。
package renderer

import (
	 "context"
	"errors"
	"sync"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
)

// ErrDownstream 表示下游数据源的瞬时失败，可重试。
var ErrDownstream = errors.New("downstream transient failure")

// ErrNotFound 表示数据源中不存在该路由，属于不可重试错误。
var ErrNotFound = errors.New("route not found in data source")

// DataSource 是渲染内容的下游来源（回源），其调用可能瞬时失败。
type DataSource interface {
	// Fetch 按路由读取原始数据。
	Fetch(ctx context.Context, route model.Route) ([]byte, error)
}

// Renderer 依据路由声明的渲染模式产出内容。产出的内容始终携带
// 路由声明的模式，绝不与其它模式混淆。
type Renderer struct {
	ds DataSource
}

// New 创建渲染器。
func New(ds DataSource) *Renderer {
	return &Renderer{ds: ds}
}

// Render 实时渲染指定路由（SSR/ISR 回源路径使用）。
func (r *Renderer) Render(ctx context.Context, route model.Route) (*model.Content, error) {
	body, err := r.ds.Fetch(ctx, route)
	if err != nil {
		return nil, err
	}
	return &model.Content{
		Route:     route.Path,
		Mode:      route.Mode,
		Body:      body,
		MediaType: "text/html; charset=utf-8",
		CreatedAt: time.Now(),
	}, nil
}

// MemoryDataSource 是一个可控的内存数据源：支持按路由设置内容、
// 注入瞬时失败（前 N 次失败后成功）或持续失败，用于测试与演示。
type MemoryDataSource struct {
	mu       sync.Mutex
	data     map[string][]byte
	failures map[string]int // 每个路由剩余的瞬时失败次数
	failForever map[string]bool
}

// NewMemoryDataSource 创建内存数据源。
func NewMemoryDataSource() *MemoryDataSource {
	return &MemoryDataSource{
		data:        make(map[string][]byte),
		failures:    make(map[string]int),
		failForever: make(map[string]bool),
	}
}

// Set 设置某路由的数据内容。
func (m *MemoryDataSource) Set(route string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[route] = body
}

// FailNext 设置某路由接下来 n 次 Fetch 都返回瞬时失败。
func (m *MemoryDataSource) FailNext(route string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[route] = n
	m.failForever[route] = false
}

// FailForever 设置某路由持续失败（返回错误，用于测试再生失败兜底）。
func (m *MemoryDataSource) FailForever(route string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failForever[route] = true
}

// Recover 清除某路由的失败注入。
func (m *MemoryDataSource) Recover(route string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failures, route)
	m.failForever[route] = false
}

// Fetch 实现 DataSource。
func (m *MemoryDataSource) Fetch(_ context.Context, route model.Route) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failForever[route.Path] {
		return nil, ErrDownstream
	}
	if n := m.failures[route.Path]; n > 0 {
		m.failures[route.Path] = n - 1
		return nil, ErrDownstream
	}
	body, ok := m.data[route.Path]
	if !ok {
		return nil, ErrNotFound
	}
	return body, nil
}
