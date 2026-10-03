package prerender

import "sync"

// PurgeEvent 是一条失效事件。事件可能乱序、重复或迟到到达，
// 最终缓存状态必须与按序全量处理一致。
type PurgeEvent struct {
	Seq     uint64 // 全局单调序号，用于去重与乱序判定
	Tag     string
	Version uint64 // 失效语义：该标签下版本 <= Version 的内容全部失效
}

// PurgeLog 记录已应用的失效事件，保证：
//   - 重复事件只生效一次（按 Seq 幂等去重）；
//   - 乱序事件不丢更新：失效语义按版本水位合并（只升不降），
//     与到达顺序无关，因此最终状态与按序全量处理一致；
//   - 迟到事件不会复活已清除的页面：失效只删版本 <= 水位的条目，
//     而 Cache.Fill 会拒绝版本不高于水位的写入。
type PurgeLog struct {
	mu        sync.Mutex
	seen      map[uint64]struct{} // 已应用的 Seq
	watermark map[string]uint64   // tag -> 已应用的失效版本水位
}

// NewPurgeLog 创建失效日志。
func NewPurgeLog() *PurgeLog {
	return &PurgeLog{
		seen:      make(map[uint64]struct{}),
		watermark: make(map[string]uint64),
	}
}

// Apply 应用一条失效事件到缓存，返回是否实际产生了失效动作。
// 重复事件返回 false；乱序与迟到事件按版本水位合并，始终安全。
func (l *PurgeLog) Apply(c *Cache, ev PurgeEvent) bool {
	l.mu.Lock()
	if _, dup := l.seen[ev.Seq]; dup {
		l.mu.Unlock()
		return false
	}
	l.seen[ev.Seq] = struct{}{}
	if ev.Version <= l.watermark[ev.Tag] {
		// 迟到或乱序的旧事件：水位已覆盖其语义，无需再作用于缓存。
		l.mu.Unlock()
		return false
	}
	l.watermark[ev.Tag] = ev.Version
	l.mu.Unlock()

	c.InvalidateTag(ev.Tag, ev.Version)
	return true
}

// Watermark 返回标签当前已应用的失效版本水位。
func (l *PurgeLog) Watermark(tag string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.watermark[tag]
}
