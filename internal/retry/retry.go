// Package retry 提供面向下游瞬时失败的有界退避重试。
package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Options 重试参数。
type Options struct {
	// MaxAttempts 最大尝试次数（含首次调用）；<=0 时取默认值 3。
	MaxAttempts int
	// InitialBackoff 首次退避时长；<=0 时取默认值 10ms。
	InitialBackoff time.Duration
	// MaxBackoff 退避时长上限；<=0 时取默认值 5s。
	MaxBackoff time.Duration
	// Multiplier 退避乘数；<=0 时取默认值 2。
	Multiplier float64
	// Jitter 抖动系数（0~1），避免重试风暴；<0 视为 0，>1 视为 1。
	Jitter float64
	// ShouldRetry 判定错误是否可重试；为 nil 时默认可重试。
	ShouldRetry func(error) bool
}

func (o *Options) normalize() {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 3
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 10 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 5 * time.Second
	}
	if o.Multiplier <= 0 {
		o.Multiplier = 2
	}
	if o.Jitter < 0 {
		o.Jitter = 0
	}
	if o.Jitter > 1 {
		o.Jitter = 1
	}
}

// Do 执行 fn，遇到可重试错误时按有界指数退避（含抖动）重试。
// 重试只包裹 fn 本身：fn 外部的缓存索引状态由调用方在 fn 成功后
// 才提交，因此反复失败不会损坏索引，也不会留下半成品条目。
// 上下文取消或退避等待被中断时返回 ctx.Err()。
func Do(ctx context.Context, opts Options, fn func() error) error {
	opts.normalize()
	var err error
	backoff := float64(opts.InitialBackoff)
	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == opts.MaxAttempts {
			break
		}
		if opts.ShouldRetry != nil && !opts.ShouldRetry(err) {
			break
		}
		// 有界指数退避 + 抖动：backoff = min(Initial * mult^(attempt-1), Max) * (1 ± jitter)
		sleep := time.Duration(backoff)
		if opts.Jitter > 0 {
			delta := backoff * opts.Jitter
			sleep = time.Duration(backoff - delta + rand.Float64()*2*delta)
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = math.Min(backoff*opts.Multiplier, float64(opts.MaxBackoff))
	}
	return err
}
