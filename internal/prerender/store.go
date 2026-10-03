package prerender

import "sync"

// ContentStore 按路由保存已提交的页面版本。
// 发布是原子的：Commit 只在完整 Page 构造完成后一次性替换已提交指针，
// 读取方只能看到某个已提交的完整版本，不会看到半成品或字段级混合。
type ContentStore struct {
	mu        sync.RWMutex
	committed map[string]*Page // route -> 当前已提交的完整版本（不可变）
}

// NewContentStore 创建空的内容存储。
func NewContentStore() *ContentStore {
	return &ContentStore{committed: make(map[string]*Page)}
}

// Commit 原子发布一个完整版本，发布后立即可被读取。
// Page 在提交前必须已完整构造；提交后不得再修改其字段。
// 版本号必须单调递增，过期的重复提交会被拒绝。
func (s *ContentStore) Commit(p *Page) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.committed[p.Route]; ok && cur.Version >= p.Version {
		return // 迟到或重复的提交不得回滚已发布的新版本
	}
	s.committed[p.Route] = p
}

// Committed 返回路由当前已提交的最新版本。
// 返回的指针指向不可变快照，读取方看到的是字段自洽的完整版本。
func (s *ContentStore) Committed(route string) (*Page, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.committed[route]
	return p, ok
}

// Version 返回路由当前已提交的版本号，未发布时返回 0。
func (s *ContentStore) Version(route string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.committed[route]; ok {
		return p.Version
	}
	return 0
}
