package prerender

import (
	"context"
	"errors"
	"time"
)

// ErrNoOrigin 表示未配置下游渲染源。
var ErrNoOrigin = errors.New("prerender: no origin configured")

// Origin 是下游渲染源（动态页面服务），调用可能瞬时失败。
type Origin interface {
	// Render 为指定路由渲染指定版本的内容。
	Render(ctx context.Context, route string, version uint64) (*Page, error)
}

// Renderer 依据路由声明的渲染模式产出页面。
// 产出页面记录的 Mode 由路由声明盖章，不得串用其它模式产出的内容。
type Renderer struct {
	origin Origin
	retry  RetryConfig
	now    func() time.Time
}

// NewRenderer 创建渲染器。
func NewRenderer(origin Origin, retry RetryConfig) *Renderer {
	return &Renderer{origin: origin, retry: retry, now: time.Now}
}

// Render 按路由配置渲染一个指定版本的新页面。
// 回源调用按有界退避重试；无论下游返回什么，Mode/Route/Tags 均以
// 路由声明为准重新盖章，保证内容与声明模式一致。
func (r *Renderer) Render(ctx context.Context, cfg RouteConfig, version uint64) (*Page, error) {
	if r.origin == nil {
		return nil, ErrNoOrigin
	}
	var page *Page
	err := DoWithRetry(ctx, r.retry, func() error {
		p, err := r.origin.Render(ctx, cfg.Path, version)
		if err != nil {
			return err
		}
		page = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	// 以路由声明为准盖章，防止下游返回的内容携带错误模式或标签。
	page.Route = cfg.Path
	page.Mode = cfg.Mode
	page.Version = version
	page.Tags = append([]string(nil), cfg.Tags...)
	page.GeneratedAt = r.now()
	return page, nil
}
