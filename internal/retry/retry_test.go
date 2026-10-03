package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDo_SuccessFirstAttempt(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Options{MaxAttempts: 3, InitialBackoff: time.Millisecond}, func() error {
		calls++
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("want nil/1 call, got err=%v calls=%d", err, calls)
	}
}

func TestDo_RetryThenSuccess(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Options{MaxAttempts: 5, InitialBackoff: time.Millisecond}, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 3 {
		t.Fatalf("want 3 calls, got %d", calls)
	}
}

func TestDo_Exhausted(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	err := Do(context.Background(), Options{MaxAttempts: 3, InitialBackoff: time.Millisecond}, func() error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("want 3 calls, got %d", calls)
	}
}

func TestDo_NonRetryableStopsEarly(t *testing.T) {
	permanent := errors.New("permanent")
	calls := 0
	err := Do(context.Background(), Options{
		MaxAttempts:   5,
		InitialBackoff: time.Millisecond,
		ShouldRetry:   func(error) bool { return false },
	}, func() error {
		calls++
		return permanent
	})
	if !errors.Is(err, permanent) || calls != 1 {
		t.Fatalf("want permanent/1 call, got err=%v calls=%d", err, calls)
	}
}

func TestDo_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := Do(ctx, Options{MaxAttempts: 5, InitialBackoff: 50 * time.Millisecond}, func() error {
		calls++
		if calls == 1 {
			cancel()
		}
		return errors.New("transient")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("want 1 call after cancel, got %d", calls)
	}
}

func TestDo_DefaultOptions(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Options{}, func() error {
		calls++
		if calls < 2 {
			return errors.New("x")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 2 {
		t.Fatalf("want 2 calls, got %d", calls)
	}
}
