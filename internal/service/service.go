// Package service 组装渲染、存储、缓存与事件，对外提供 HTTP 服务。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/cache"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/events"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/model"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/renderer"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/retry"
	"github.com/huchangjun118/prerender-cdn-cache-go/internal/store"
)

// 响应头常量：读取方可以直接从响应状态/头观察缓存行为。
const (
	HeaderCache       = "X-Cache"
	HeaderRenderMode  = "X-Render-Mode"
	HeaderVersion     = "X-Content-Version"
	HeaderWarning     = "Warning"
	HeaderContentMode = "X-Content-Mode"

	CacheHit     = "HIT"
	CacheMiss    = "MISS"
	CacheStale   = "STALE"
	CacheExpired = "EXPIRED"

	// Warning 110 (Response is stale)：陈旧窗口内返回了陈旧内容。
	WarnStale = `110 - "Response is stale"`
	// Warning 111 (Revalidation failed)：再生失败且无陈旧可返回。
	WarnRevalidationFailed = `111 - "Revalidation failed"`
)

// 服务内部错误。
var (
	errRouteNotFound   = errors.New("route not registered")
	errNoPublished     = errors.New("no published content")
	errRegenFailed     = errors.New("regeneration failed")
	errContentChanged  = errors.New("content changed during render")
)

// Config 服务配置。
type Config struct {
	// DefaultFreshTTL ISR 条目的默认新鲜时长。
	DefaultFreshTTL time.Duration
	// DefaultStaleWindow ISR 默认陈旧窗口。
	DefaultStaleWindow time.Duration
	// Retry 回源/渲染失败的重试参数。
	Retry retry.Options
	// Clock 注入时钟（测试用）。
	Clock func() time.Time
	// AsyncEvents 异步投递失效事件。
	AsyncEvents bool
}

func (c *Config) normalize() {
	if c.DefaultFreshTTL <= 0 {
		c.DefaultFreshTTL = time.Minute
	}
	if c.DefaultStaleWindow <= 0 {
		c.DefaultStaleWindow = 5 * time.Minute
	}
	if c.Retry.MaxAttempts <= 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.InitialBackoff <= 0 {
		c.Retry.InitialBackoff = 20 * time.Millisecond
	}
	if c.Retry.MaxBackoff <= 0 {
		c.Retry.MaxBackoff = 500 * time.Millisecond
	}
	if c.Retry.Multiplier <= 0 {
		c.Retry.Multiplier = 2
	}
	if c.Retry.Jitter <= 0 {
		c.Retry.Jitter = 0.2
	}
	if c.Retry.ShouldRetry == nil {
		c.Retry.ShouldRetry = func(err error) bool {
			// 内层重试只针对下游瞬时失败；内容变更（errContentChanged）
			// 由 regenerate 的外层循环立即重试，不做退避等待。
			return errors.Is(err, renderer.ErrDownstream)
		}
	}
}

// regenCall 合并同一路由的并发再生请求（single-flight）。
type regenCall struct {
	done    chan struct{}
	content *model.Content
	err     error
}

// Service 预渲染与边缘缓存服务。
type Service struct {
	cfg      Config
	store    *store.Store
	cache    *cache.Cache
	events   *events.Bus
	renderer *renderer.Renderer
	ds       *renderer.MemoryDataSource

	mu       sync.RWMutex
	routes   map[string]model.Route
	inflight map[string]*regenCall
}

// New 创建服务实例。
func New(cfg Config) *Service {
	cfg.normalize()
	ds := renderer.NewMemoryDataSource()
	var clock func() time.Time
	if cfg.Clock != nil {
		clock = cfg.Clock
	}
	s := &Service{
		cfg:      cfg,
		store:    store.New(),
		ds:       ds,
		renderer: renderer.New(ds),
		routes:   make(map[string]model.Route),
		inflight: make(map[string]*regenCall),
	}
	if clock != nil {
		s.cache = cache.New(cache.WithClock(clock))
	} else {
		s.cache = cache.New()
	}
	if cfg.AsyncEvents {
		s.events = events.New(s.cache, events.WithAsync(1024))
	} else {
		s.events = events.New(s.cache)
	}
	return s
}

// Store 返回内容存储。
func (s *Service) Store() *store.Store { return s.store }

// Cache 返回边缘缓存。
func (s *Service) Cache() *cache.Cache { return s.cache }

// Events 返回失效事件总线。
func (s *Service) Events() *events.Bus { return s.events }

// Renderer 返回渲染器。
func (s *Service) Renderer() *renderer.Renderer { return s.renderer }

// DataSource 返回内存数据源（测试/演示用）。
func (s *Service) DataSource() *renderer.MemoryDataSource { return s.ds }

// Now 返回服务当前时钟。
func (s *Service) Now() time.Time { return s.cache.Now() }

// normalizePath 规范化路由路径。
func normalizePath(p string) string {
	p = strings.TrimSpace(p)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// Route 注册或更新一条路由声明（零值字段填默认值）。
func (s *Service) Route(r model.Route) {
	r.Path = normalizePath(r.Path)
	if r.FreshTTL <= 0 {
		r.FreshTTL = s.cfg.DefaultFreshTTL
	}
	if r.StaleWindow <= 0 {
		r.StaleWindow = s.cfg.DefaultStaleWindow
	}
	if r.ErrorPolicy == "" {
		r.ErrorPolicy = model.ErrorPolicyStaleIfError
	}
	s.mu.Lock()
	s.routes[r.Path] = r
	s.mu.Unlock()
}

func (s *Service) getRoute(path string) (model.Route, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.routes[path]
	return r, ok
}

// Publish 发布内容：更新数据源、渲染、按版本原子提交、按标签定向失效。
// 返回已提交的内容版本。发布与失效期间，读取方只会看到某个完整版本。
func (s *Service) Publish(ctx context.Context, path string, body []byte) (*model.Content, error) {
	path = normalizePath(path)
	route, ok := s.getRoute(path)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errRouteNotFound, path)
	}

	// 1. 更新下游数据源（内容变更）。
	s.ds.Set(path, body)

	// 2. 渲染新内容（SSG 为构建期预生成；ISR/SSR 发布时同样渲染一份，
	//    保证数据源与已提交版本一致）。渲染在锁外进行，不阻塞读取。
	content, err := s.renderer.Render(ctx, route)
	if err != nil {
		return nil, fmt.Errorf("render: %w", err)
	}

	// 3. 原子提交：全局版本号与标签版本向量在锁内一次性推进，
	//    内容整体切换，不存在字段级混合。
	if _, err := s.store.Publish(ctx, content, route.Tags); err != nil {
		return nil, err
	}

	// 4. 按标签定向失效：事件携带 (标签, 新版本号)，仅失效依赖版本
	//    严格小于新版本的条目；其它标签条目继续命中，不清空整站。
	for _, tag := range route.Tags {
		ev := model.InvalidationEvent{
			Tag:        tag,
			Version:    content.Deps[tag],
			EventID:    fmt.Sprintf("%s#%d", tag, content.Deps[tag]),
			OccurredAt: s.Now(),
		}
		s.events.Publish(ev)
	}

	// 5. SSG 内容不可变：发布后直接放入缓存（新鲜期视为很长，
	//    读取路径仍以 store 为准，缓存条目用于观测与模式校验）。
	if route.Mode == model.ModeSSG {
		s.cache.Put(cache.NewEntry(route, content, s.Now()))
	}

	return content, nil
}

// regenerate 回源渲染一份新内容，带回溯重试与“渲染期间内容已变更”检测：
// 若渲染期间标签版本推进（有新发布），丢弃本次结果并重试，保证缓存
// 内容与其依赖版本严格一致，已失效的旧内容不会借新条目复活。
func (s *Service) regenerate(ctx context.Context, route model.Route) (*model.Content, error) {
	var content *model.Content
	// errContentChanged 表示渲染期间有新发布（内容已过期），它不是下游
	// 失败：给一个独立的宽松重试预算，保证持续发布风暴下仍能收敛到
	// 某个已提交版本，而不是耗尽重试后报错。
	const maxChangeAttempts = 20
	var lastErr error
	for attempt := 0; attempt < maxChangeAttempts; attempt++ {
		var c *model.Content
		var err error
		err = retry.Do(ctx, s.cfg.Retry, func() error {
			depsBefore := s.store.TagVersions(route.Tags)
			c, err = s.renderer.Render(ctx, route)
			if err != nil {
				return err
			}
			depsAfter := s.store.TagVersions(route.Tags)
			if !depsEqual(depsBefore, depsAfter) {
				return errContentChanged
			}
			return nil
		})
		if err == nil {
			content = c
			break
		}
		if !errors.Is(err, errContentChanged) {
			// 下游失败且内层重试已耗尽：直接返回。
			return nil, err
		}
		lastErr = err
	}
	if content == nil {
		return nil, lastErr
	}
	// 以当前标签版本盖棺定论；若盖棺后又有发布，缓存侧高水位会将
	// 该条目判为失效，读取方随后再生成即可。
	content.Deps = s.store.TagVersions(route.Tags)
	if cur, ok := s.store.Current(route.Path); ok {
		content.Version = cur.Version
	}
	return content, nil
}

func depsEqual(a, b model.VersionVector) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// triggerBackgroundRegen 触发后台增量再生（single-flight 合并同路由并发）。
// 非阻塞：若同路由已有再生在进行，直接返回，不延迟已写出的 STALE 响应。
func (s *Service) triggerBackgroundRegen(route model.Route) {
	s.mu.Lock()
	if _, ok := s.inflight[route.Path]; ok {
		s.mu.Unlock()
		return
	}
	call := &regenCall{done: make(chan struct{})}
	s.inflight[route.Path] = call
	s.mu.Unlock()

	go func() {
		defer func() {
			close(call.done)
			s.mu.Lock()
			delete(s.inflight, route.Path)
			s.mu.Unlock()
		}()
		// 后台再生脱离请求生命周期，使用独立 context。
		c, err := s.regenerate(context.Background(), route)
		if err != nil {
			log.Printf("background regen %s failed: %v", route.Path, err)
			return
		}
		s.cache.Put(cache.NewEntry(route, c, s.Now()))
	}()
}

// serveContent 写出内容并设置公共响应头。
func serveContent(w http.ResponseWriter, route model.Route, content *model.Content, cacheStatus string, warning string) {
	h := w.Header()
	h.Set(HeaderCache, cacheStatus)
	h.Set(HeaderRenderMode, string(route.Mode))
	h.Set(HeaderContentMode, string(content.Mode))
	h.Set(HeaderVersion, fmt.Sprintf("%d", content.Version))
	if warning != "" {
		h.Set(HeaderWarning, warning)
	}
	if content.MediaType != "" {
		h.Set("Content-Type", content.MediaType)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content.Body)
}

// handleRender 处理页面读取。
func (s *Service) handleRender(w http.ResponseWriter, r *http.Request) {
	path := normalizePath(strings.TrimPrefix(r.URL.Path, "/render"))
	route, ok := s.getRoute(path)
	if !ok {
		http.Error(w, fmt.Sprintf("route %q not registered", path), http.StatusNotFound)
		return
	}

	switch route.Mode {
	case model.ModeSSG:
		// SSG：只读构建期已发布版本，永不触发渲染。
		content, ok := s.store.Current(path)
		if !ok {
			http.Error(w, "page not prerendered yet", http.StatusServiceUnavailable)
			return
		}
		serveContent(w, route, content, CacheHit, "")

	case model.ModeSSR:
		// SSR：每次请求实时渲染，不读缓存、不写缓存（模式隔离）。
		content, err := s.regenerate(r.Context(), route)
		if err != nil {
			w.Header().Set(HeaderCache, CacheMiss)
			w.Header().Set(HeaderRenderMode, string(route.Mode))
			w.Header().Set(HeaderWarning, WarnRevalidationFailed)
			http.Error(w, "render failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		serveContent(w, route, content, CacheMiss, "")

	case model.ModeISR:
		s.serveISR(w, r, route)

	default:
		http.Error(w, "unsupported render mode", http.StatusInternalServerError)
	}
}

// serveISR 处理 ISR 读取：fresh 直接命中；stale 窗口内返回陈旧并后台
// 再生；expired/invalidated/未命中则同步再生，失败时按既定策略在
// 陈旧内容与错误之间做出选择，行为可从状态/响应头观察。
func (s *Service) serveISR(w http.ResponseWriter, r *http.Request, route model.Route) {
	entry, state, found := s.cache.Get(route.Path)
	if found && entry.Mode() != route.Mode {
		// 模式不一致：该条目不是本模式产出的页面，不得当作命中。
		s.cache.Delete(route.Path)
		entry, state, found = nil, cache.StateExpired, false
	}

	if found {
		switch state {
		case cache.StateFresh:
			serveContent(w, route, entry.Content(), CacheHit, "")
			return
		case cache.StateStale:
			// 陈旧窗口内：立即返回陈旧内容，后台静默再生。
			serveContent(w, route, entry.Content(), CacheStale, WarnStale)
			s.triggerBackgroundRegen(route)
			return
		case cache.StateInvalidated:
			// 已失效：不得返回，走同步再生。
		case cache.StateExpired:
			// 超出陈旧窗口：走同步再生。
		}
	}

	// 未命中 / 已失效 / 已过期：同步再生。
	content, err := s.regenerate(r.Context(), route)
	if err == nil {
		s.cache.Put(cache.NewEntry(route, content, s.Now()))
		serveContent(w, route, content, CacheMiss, "")
		return
	}

	// 再生失败：按既定策略在“受限陈旧窗口”与“返回错误”间抉择。
	if route.ErrorPolicy == model.ErrorPolicyStaleIfError && found && entry != nil {
		if state == cache.StateStale {
			// 陈旧窗口内：返回陈旧内容，调用方可从 STALE + Warning 观察。
			serveContent(w, route, entry.Content(), CacheStale, WarnStale)
			s.triggerBackgroundRegen(route)
			return
		}
	}
	// 窗口外 / 已失效 / 策略为直接报错：返回错误，EXPIRED + Warning 可观察。
	w.Header().Set(HeaderCache, CacheExpired)
	w.Header().Set(HeaderRenderMode, string(route.Mode))
	w.Header().Set(HeaderWarning, WarnRevalidationFailed)
	http.Error(w, "regeneration failed: "+err.Error(), http.StatusGatewayTimeout)
}

// handlePublish 处理内容发布。
func (s *Service) handlePublish(w http.ResponseWriter, r *http.Request) {
	path := normalizePath(r.URL.Query().Get("route"))
	body, err := readAllLimited(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	content, err := s.Publish(r.Context(), path, body)
	if err != nil {
		if errors.Is(err, errRouteNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"route":   content.Route,
		"mode":    content.Mode,
		"version": content.Version,
		"deps":    content.Deps,
	})
}

// handleInvalidate 处理手动失效事件提交。
func (s *Service) handleInvalidate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Tag     string `json:"tag"`
		Version int64  `json:"version"`
		EventID string `json:"event_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Tag == "" {
		http.Error(w, "tag required", http.StatusBadRequest)
		return
	}
	if req.Version <= 0 {
		req.Version = s.store.TagVersion(req.Tag)
	}
	if req.EventID == "" {
		req.EventID = fmt.Sprintf("manual-%s#%d-%d", req.Tag, req.Version, time.Now().UnixNano())
	}
	s.events.Publish(model.InvalidationEvent{
		Tag:        req.Tag,
		Version:    req.Version,
		EventID:    req.EventID,
		OccurredAt: s.Now(),
	})
	if s.cfg.AsyncEvents {
		s.events.Flush()
	}
	_, processed, dups, invalidated := s.events.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"tag":                req.Tag,
		"version":            req.Version,
		"processed_events":   processed,
		"duplicate_events":   dups,
		"invalidated_entries": invalidated,
	})
}

// handleFailures 注入/清除数据源失败（测试与演示用）。
func (s *Service) handleFailures(w http.ResponseWriter, r *http.Request) {
	path := normalizePath(r.URL.Query().Get("route"))
	forever := r.URL.Query().Get("forever") == "1"
	n := atoiDefault(r.URL.Query().Get("n"), 0)
	if forever {
		s.ds.FailForever(path)
	} else if n > 0 {
		s.ds.FailNext(path, n)
	} else {
		s.ds.Recover(path)
	}
	writeJSON(w, http.StatusOK, map[string]any{"route": path, "forever": forever, "fail_next": n})
}

// handleAdminCache 返回缓存索引快照。
func (s *Service) handleAdminCache(w http.ResponseWriter, r *http.Request) {
	entries := s.cache.Entries()
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{
			"route":       e.Content().Route,
			"mode":        e.Mode(),
			"state":       e.State().String(),
			"version":     e.Content().Version,
			"deps":        e.Deps(),
			"age_ms":      s.Now().Sub(e.CreatedAt()).Milliseconds(),
			"fresh_until": e.FreshUntil().Sub(s.Now()).Milliseconds(),
			"stale_until": e.StaleUntil().Sub(s.Now()).Milliseconds(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"now":     s.Now(),
		"entries": out,
	})
}

// handleAdminStore 返回已提交版本快照。
func (s *Service) handleAdminStore(w http.ResponseWriter, r *http.Request) {
	routes := s.store.Routes()
	out := make([]map[string]any, 0, len(routes))
	for _, path := range routes {
		c, ok := s.store.Current(path)
		if !ok {
			continue
		}
		out = append(out, map[string]any{
			"route":   c.Route,
			"mode":    c.Mode,
			"version": c.Version,
			"deps":    c.Deps,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": out})
}

// handleAdminClear 清空缓存索引（全量重建用）。
func (s *Service) handleAdminClear(w http.ResponseWriter, r *http.Request) {
	s.cache.Clear()
	writeJSON(w, http.StatusOK, map[string]any{"cleared": true})
}

// handleWarmUp 全量预热：清空缓存后对所有 ISR 路由同步再生，
// 用于验证最终缓存状态与全量重建一致。
func (s *Service) handleWarmUp(w http.ResponseWriter, r *http.Request) {
	s.cache.Clear()
	s.mu.RLock()
	routes := make([]model.Route, 0, len(s.routes))
	for _, route := range s.routes {
		routes = append(routes, route)
	}
	s.mu.RUnlock()

	var errs []string
	for _, route := range routes {
		if route.Mode != model.ModeISR {
			continue
		}
		content, err := s.regenerate(r.Context(), route)
		if err != nil {
			errs = append(errs, route.Path+": "+err.Error())
			continue
		}
		s.cache.Put(cache.NewEntry(route, content, s.Now()))
	}
	if len(errs) > 0 {
		writeJSON(w, http.StatusBadGateway, map[string]any{"errors": errs})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"warmed": len(routes)})
}

// handleAdminRoutes 注册/更新路由声明。
func (s *Service) handleAdminRoutes(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path         string   `json:"path"`
		Mode         string   `json:"mode"`
		Tags         []string `json:"tags"`
		FreshTTLMS   int64    `json:"fresh_ttl_ms"`
		StaleWindowMS int64   `json:"stale_window_ms"`
		ErrorPolicy  string   `json:"error_policy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if model.RenderMode(req.Mode).Valid() {
		// ok
	} else {
		http.Error(w, "invalid mode: "+req.Mode, http.StatusBadRequest)
		return
	}
	route := model.Route{
		Path:        req.Path,
		Mode:        model.RenderMode(req.Mode),
		Tags:        req.Tags,
		FreshTTL:    time.Duration(req.FreshTTLMS) * time.Millisecond,
		StaleWindow: time.Duration(req.StaleWindowMS) * time.Millisecond,
		ErrorPolicy: model.ErrorPolicy(req.ErrorPolicy),
	}
	s.Route(route)
	writeJSON(w, http.StatusOK, map[string]any{"registered": route.Path, "mode": route.Mode})
}

// Handler 返回 HTTP 处理器。
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/render", s.handleRender)
	mux.HandleFunc("/render/", s.handleRender)
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/invalidate", s.handleInvalidate)
	mux.HandleFunc("/admin/routes", s.handleAdminRoutes)
	mux.HandleFunc("/admin/failures", s.handleFailures)
	mux.HandleFunc("/admin/cache", s.handleAdminCache)
	mux.HandleFunc("/admin/store", s.handleAdminStore)
	mux.HandleFunc("/admin/clear", s.handleAdminClear)
	mux.HandleFunc("/admin/warmup", s.handleWarmUp)
	return mux
}

// --- 小工具 ---

func readAllLimited(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}
