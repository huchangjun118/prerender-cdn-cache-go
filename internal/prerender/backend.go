package prerender

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Backend 是边缘缓存的底层存储，调用可能瞬时失败，需要按有界退避重试。
type Backend interface {
	Get(key string) (*Page, bool)
	Set(key string, p *Page) error
	Delete(key string) error
	Keys() []string
}

// MemoryBackend 是进程内 Backend 实现。
type MemoryBackend struct {
	mu   sync.RWMutex
	data map[string]*Page
}

// NewMemoryBackend 创建内存后端。
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{data: make(map[string]*Page)}
}

func (m *MemoryBackend) Get(key string) (*Page, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.data[key]
	return p, ok
}

func (m *MemoryBackend) Set(key string, p *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = p
	return nil
}

func (m *MemoryBackend) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MemoryBackend) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// RetryConfig 控制有界退避重试。
type RetryConfig struct {
	MaxAttempts int           // 最大尝试次数（含首次），<=0 时按 1 处理
	BaseDelay   time.Duration // 首次重试前的等待
	MaxDelay    time.Duration // 退避上限
}

// DoWithRetry 按指数退避（有界）重试 fn，直到成功、次数耗尽或 ctx 取消。
// 重试本身不改变任何外部状态，反复失败只返回错误，由调用方保证不留下
// 半成品状态（例如缓存索引只在成功回写后才登记）。
func DoWithRetry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	attempts := cfg.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	delay := cfg.BaseDelay
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			delay *= 2
			if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}
	}
	return err
}
