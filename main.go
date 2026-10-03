// Command prerender-cdn-cache-go 启动预渲染与边缘缓存服务。
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/huchangjun118/prerender-cdn-cache-go/internal/service"
)

func main() {
	addr := flag.String("addr", ":8080", "监听地址")
	flag.Parse()

	svc := service.New(service.Config{})
	log.Printf("prerender cache service listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, svc.Handler()))
}
