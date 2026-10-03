// Package model 定义预渲染与边缘缓存系统的核心数据模型。
package model

import "time"

// RenderMode 渲染模式。同一路由在同一时刻只能以一种模式产出内容，
// 缓存条目与读取路径都必须携带并校验模式，不得串用其它模式产出的页面。
type RenderMode string

const (
	// ModeSSG 静态预渲染：构建期生成并发布，读取永不触发渲染。
	ModeSSG RenderMode = "ssg"
	// ModeSSR 服务端实时渲染：每次请求都回源渲染，不做页面缓存。
	ModeSSR RenderMode = "ssr"
	// ModeISR 增量再生：发布时失效、请求时在陈旧窗口内后台再生。
	ModeISR RenderMode = "isr"
)

// Valid 判断模式是否受支持。
func (m RenderMode) Valid() bool {
	switch m {
	case ModeSSG, ModeSSR, ModeISR:
		return true
	}
	return false
}

// ErrorPolicy 描述 ISR 再生失败时的既定策略。
type ErrorPolicy string

const (
	// ErrorPolicyStaleIfError 在受限的陈旧窗口内继续返回陈旧内容，
	// 超出窗口则返回错误。
	ErrorPolicyStaleIfError ErrorPolicy = "stale-if-error"
	// ErrorPolicyError 再生失败直接返回错误，不返回陈旧内容。
	ErrorPolicyError ErrorPolicy = "error"
)

// Route 是一条路由的声明，包含渲染模式、标签与缓存策略。
type Route struct {
	Path string
	// Mode 该路由的渲染模式，读取内容必须与此一致。
	Mode RenderMode
	// Tags 内容标签，用于定向失效。
	Tags []string
	// FreshTTL ISR 条目保持新鲜的时长。
	FreshTTL time.Duration
	// StaleWindow ISR 陈旧窗口：窗口内允许返回陈旧内容（后台再生或再生失败兜底）。
	StaleWindow time.Duration
	// ErrorPolicy 再生失败策略，零值取 ErrorPolicyStaleIfError。
	ErrorPolicy ErrorPolicy
}

// VersionVector 是标签 -> 该标签内容版本的向量。内容发布时记录其依赖的
// 每个标签的版本；失效事件携带 (标签, 新版本号)。缓存条目仅当某个标签的
// 依赖版本严格小于事件版本时才失效，从而乱序/重复/迟到事件都不会复活
// 已清除的内容，也不会误伤更新的内容。
type VersionVector map[string]int64

// Content 是一次渲染产出的不可变内容。发布即整体替换当前版本指针，
// 读取方要么看到完整的旧版本，要么看到完整的新版本，不存在字段级混合。
type Content struct {
	// Route 所属路由路径。
	Route string
	// Mode 产出该内容的渲染模式。
	Mode RenderMode
	// Body 内容字节。
	Body []byte
	// MediaType 内容类型。
	MediaType string
	// Version 内容的全局发布序号。
	Version int64
	// Deps 内容依赖的标签版本向量。
	Deps VersionVector
	// CreatedAt 产出时间。
	CreatedAt time.Time
}

// InvalidationEvent 是内容变更后发出的定向失效事件。
type InvalidationEvent struct {
	// Tag 失效标签。
	Tag string
	// Version 该标签变更后的新版本号。
	Version int64
	// EventID 事件唯一标识，用于去重与观测。
	EventID string
	// OccurredAt 事件发生时间。
	OccurredAt time.Time
}
