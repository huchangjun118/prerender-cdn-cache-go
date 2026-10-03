// Package prerender 提供面向动态页面的预渲染与边缘缓存能力：
// SSG/SSR/ISR 三种渲染模式共存、按版本原子发布、按标签定向失效、
// 受限陈旧窗口与有界退避重试。
package prerender

import "time"

// RenderMode 声明路由的渲染模式。
type RenderMode string

const (
	// ModeSSG 构建期预生成：发布时渲染并提交，运行时只读缓存。
	ModeSSG RenderMode = "ssg"
	// ModeSSR 请求时实时渲染：每次请求回源渲染，不写入缓存。
	ModeSSR RenderMode = "ssr"
	// ModeISR 增量再生：请求时若缓存失效则渲染并回写缓存。
	ModeISR RenderMode = "isr"
)

// StalePolicy 决定再生失败时在"返回陈旧内容"与"返回错误"之间的选择。
type StalePolicy int

const (
	// StalePolicyServeStale 再生失败且处于陈旧窗口内时返回陈旧内容。
	StalePolicyServeStale StalePolicy = iota
	// StalePolicyFail 再生失败时直接返回错误，即使存在陈旧内容。
	StalePolicyFail
)

// Page 是某个路由某个已提交版本的完整页面快照。
// Page 一旦创建即不可变，读取方只能看到某个完整版本。
type Page struct {
	Route       string
	Mode        RenderMode
	Version     uint64
	Body        []byte
	Tags        []string
	GeneratedAt time.Time
}

// RouteConfig 是单条路由的声明式配置。
type RouteConfig struct {
	Path        string
	Mode        RenderMode
	FreshTTL    time.Duration // 新鲜期：期间缓存直接命中
	StaleTTL    time.Duration // 陈旧窗口：新鲜期结束后仍可用于失败兜底
	StalePolicy StalePolicy
	Tags        []string
}

// CacheStatus 描述一次缓存读取的结果，可通过响应头观察。
type CacheStatus string

const (
	StatusHit   CacheStatus = "HIT"   // 新鲜命中
	StatusStale CacheStatus = "STALE" // 再生失败，返回陈旧内容
	StatusMiss  CacheStatus = "MISS"  // 未命中，已实时渲染
	StatusError CacheStatus = "ERROR" // 渲染失败且策略不允许返回陈旧内容
)
