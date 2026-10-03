package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/prerender"
)

// demoOrigin 是一个演示用下游渲染源，真实部署中替换为动态页面服务。
type demoOrigin struct{}

func (demoOrigin) Render(_ context.Context, route string, version uint64) (*prerender.Page, error) {
	body := fmt.Sprintf("<html><body><h1>%s</h1><p>version=%d rendered at %s</p></body></html>",
		route, version, time.Now().UTC().Format(time.RFC3339))
	return &prerender.Page{Body: []byte(body)}, nil
}

func main() {
	retry := prerender.RetryConfig{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second}
	backend := prerender.NewMemoryBackend()
	store := prerender.NewContentStore()
	renderer := prerender.NewRenderer(demoOrigin{}, retry)
	cache := prerender.NewCache(backend, store, renderer, retry)
	h := prerender.NewHandler(cache, store, renderer)

	addr := ":8080"
	log.Printf("prerender-cdn-cache listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h.Mux()))
}
