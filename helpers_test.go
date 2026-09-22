package eventqueue

import (
	"context"
	"sync"
	"testing"
	"time"
)

func testConfig(t testing.TB, dir string, topics ...TopicConfig) Config {
	t.Helper()
	return Config{
		DataDir:       dir,
		MaxMemorySize: 1 << 20,
		Topics:        topics,
	}
}

func defaultTopic() TopicConfig {
	return TopicConfig{
		Name:         "orders",
		PartitionNum: 2,
		Backpressure: BackpressureConfig{MaxPartitionLag: 100, Mode: BackpressureBlock},
		Retry: RetryPolicy{
			MaxRetries:  3,
			BaseBackoff: time.Millisecond,
			MaxBackoff:  10 * time.Millisecond,
			Multiplier:  2,
		},
	}
}

func newTestBroker(t testing.TB, cfg Config) *Broker {
	t.Helper()
	b, err := Open(cfg)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})
	return b
}

// waitFor 轮询条件直到为真或超时。
func waitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

// recorded 是并发安全的消费记录收集器。
type recorded struct {
	mu      sync.Mutex
	order   []string // "topic:part:offset"
	payload map[string]int
}

func newRecorded() *recorded {
	return &recorded{payload: map[string]int{}}
}

func (r *recorded) add(ev *Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, evKey(ev))
	r.payload[evKey(ev)]++
}

func evKey(ev *Event) string {
	return ev.Topic + ":" + itoa(ev.Partition) + ":" + itoa64(ev.Offset)
}

func itoa(n int) string { return itoa64(int64(n)) }
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func (r *recorded) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

func (r *recorded) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.payload[key]
}
