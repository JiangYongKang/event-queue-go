package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func errorIs(err, target error) bool { return errors.Is(err, target) }

func testConfig(dir string) Config {
	return Config{
		DataDir:                 dir,
		NumPartitions:           1,
		MaxBacklogPerPartition:  0, // 默认不限流
		MaxMemoryBytes:          0,
		BackpressurePolicy:      BackpressureReject,
		MaxInFlightPerPartition: 1,
		Retry: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			Multiplier:     2,
		},
	}
}

// serve 使用 h 处理收到的事件：nil 结果 Ack，非 nil 结果 Nack，直到 ctx 取消。
func serve(ctx context.Context, b *Broker, c *Consumer, h Handler, wg *sync.WaitGroup, errs *[]error, mu *sync.Mutex) {
	if wg != nil {
		defer wg.Done()
	}
	for {
		ev, err := c.Poll(ctx)
		if err != nil {
			return
		}
		herr := h(ctx, ev)
		if herr == nil {
			err = c.Ack(ev)
		} else {
			err = c.Nack(ev, herr)
		}
		if err != nil && !errors.Is(err, ErrClosed) {
			if mu != nil {
				mu.Lock()
			}
			if errs != nil {
				*errs = append(*errs, err)
			}
			if mu != nil {
				mu.Unlock()
			}
		}
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}
