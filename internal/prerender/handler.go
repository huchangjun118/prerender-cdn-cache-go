package prerender

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

// 响应头：缓存行为必须可从响应状态或响应头观察。
const (
	HeaderCacheStatus    = "X-Cache"           // HIT / MISS / STALE / ERROR
	HeaderContentVersion = "X-Content-Version" // 实际返回内容的版本号
	HeaderRenderMode     = "X-Render-Mode"     // 产出该内容的渲染模式
)

// Handler 暴露页面读取、路由声明、发布与失效的 HTTP API。
type Handler struct {
	cache    *Cache
	store    *ContentStore
	renderer *Renderer
	purgeLog *PurgeLog

	mu     sync.RWMutex
	routes map[string]RouteConfig
}

// NewHandler 创建 HTTP 处理器。
func NewHandler(cache *Cache, store *ContentStore, renderer *Renderer) *Handler {
	return &Handler{
		cache:    cache,
		store:    store,
		renderer: renderer,
		purgeLog: NewPurgeLog(),
		routes:   make(map[string]RouteConfig),
	}
}

// PurgeLog 暴露失效日志，便于接入外部事件源。
func (h *Handler) PurgeLog() *PurgeLog { return h.purgeLog }

// Mux 返回注册好路由的 ServeMux。
func (h *Handler) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /pages/{route...}", h.handleGetPage)
	mux.HandleFunc("PUT /routes/{route...}", h.handlePutRoute)
	mux.HandleFunc("POST /publish/{route...}", h.handlePublish)
	mux.HandleFunc("POST /invalidate", h.handleInvalidate)
	return mux
}

type routeRequest struct {
	Mode        string   `json:"mode"`
	FreshTTLMs  int64    `json:"freshTTLMs"`
	StaleTTLMs  int64    `json:"staleTTLMs"`
	StalePolicy string   `json:"stalePolicy"` // "serve-stale" | "fail"
	Tags        []string `json:"tags"`
}

func (h *Handler) handlePutRoute(w http.ResponseWriter, r *http.Request) {
	var req routeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	cfg := RouteConfig{
		Path:     r.PathValue("route"),
		Mode:     RenderMode(req.Mode),
		FreshTTL: time.Duration(req.FreshTTLMs) * time.Millisecond,
		StaleTTL: time.Duration(req.StaleTTLMs) * time.Millisecond,
		Tags:     req.Tags,
	}
	switch req.StalePolicy {
	case "", "serve-stale":
		cfg.StalePolicy = StalePolicyServeStale
	case "fail":
		cfg.StalePolicy = StalePolicyFail
	default:
		http.Error(w, "unknown stalePolicy", http.StatusBadRequest)
		return
	}
	switch cfg.Mode {
	case ModeSSG, ModeSSR, ModeISR:
	default:
		http.Error(w, "unknown mode", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.routes[cfg.Path] = cfg
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) routeConfig(route string) (RouteConfig, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cfg, ok := h.routes[route]
	return cfg, ok
}

func (h *Handler) handleGetPage(w http.ResponseWriter, r *http.Request) {
	route := r.PathValue("route")
	cfg, ok := h.routeConfig(route)
	if !ok {
		http.Error(w, "route not declared", http.StatusNotFound)
		return
	}
	page, status, err := h.cache.Get(r.Context(), cfg)
	w.Header().Set(HeaderCacheStatus, string(status))
	w.Header().Set(HeaderRenderMode, string(cfg.Mode))
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, ErrNotPublished) {
			code = http.StatusNotFound
		}
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set(HeaderContentVersion, itoa(page.Version))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(page.Body)
}

// handlePublish 渲染并原子发布一个新版本。
// SSG 路由同时回填缓存；ISR/SSR 路由仅提交版本，
// 由后续请求按各自模式获取。
func (h *Handler) handlePublish(w http.ResponseWriter, r *http.Request) {
	route := r.PathValue("route")
	cfg, ok := h.routeConfig(route)
	if !ok {
		http.Error(w, "route not declared", http.StatusNotFound)
		return
	}
	version := h.store.Version(route) + 1
	page, err := h.renderer.Render(r.Context(), cfg, version)
	if err != nil {
		w.Header().Set(HeaderCacheStatus, string(StatusError))
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	h.store.Commit(page)
	if cfg.Mode == ModeSSG {
		if err := h.cache.Fill(page, cfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	w.Header().Set(HeaderContentVersion, itoa(version))
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"route": route, "version": version})
}

type invalidateRequest struct {
	Seq     uint64 `json:"seq"`
	Tag     string `json:"tag"`
	Version uint64 `json:"version"`
}

func (h *Handler) handleInvalidate(w http.ResponseWriter, r *http.Request) {
	var req invalidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	applied := h.purgeLog.Apply(h.cache, PurgeEvent{
		Seq:     req.Seq,
		Tag:     req.Tag,
		Version: req.Version,
	})
	w.Header().Set(HeaderContentVersion, itoa(h.purgeLog.Watermark(req.Tag)))
	_ = json.NewEncoder(w).Encode(map[string]any{"applied": applied})
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
