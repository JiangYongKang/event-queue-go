package queue

import (
	"context"
	"testing"
	"time"
)

// TestBackpressureReject 积压达到单分区上限时，Reject 策略立即拒绝并计数。
func TestBackpressureReject(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 3
	cfg.BackpressurePolicy = BackpressureReject
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("bp", 1); err != nil {
		t.Fatal(err)
	}

	c, err := b.Subscribe("bp", "g", "c0")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 先订阅让组参与背压计算（初始 committed=0）。
	for i := 0; i < 3; i++ {
		if _, _, err := b.Publish(ctx, "bp", "k", []byte("x")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	// 第四个必须被拒绝，内存不会无界增长，事件也未被静默丢弃。
	if _, _, err := b.Publish(ctx, "bp", "k", []byte("x")); !errorIs(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	if b.Stats().Rejected != 1 {
		t.Fatalf("Rejected=%d, want 1", b.Stats().Rejected)
	}
	m := b.Backlog()[0]
	if m.Pending != 3 || m.PayloadBytes != 3 {
		t.Fatalf("metrics=%+v, want Pending=3 Bytes=3", m)
	}
	t.Logf("判定依据: 上限=3，第 4 次发布返回 ErrBackpressure 且 Rejected=1；指标=%+v（无静默丢弃）", m)

	// 消费并确认一个事件后，积压腾出，发布解除阻塞。
	ev, err := c.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ev); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, _, e := publishNow(b, "bp", "k", []byte("x"))
		return e == nil
	}, "publish succeeds after ack frees slot")
	if got := b.Backlog()[0]; got.Pending != 3 {
		t.Fatalf("Pending=%d after refill, want 3", got.Pending)
	}
	t.Logf("关键位点: committed=%d，确认后发布恢复，Pending 回到上限 3",
		b.topics["bp"].partitions[0].committedOffset("g"))
}

func publishNow(b *Broker, topic, key string, p []byte) (int, uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return b.Publish(ctx, topic, key, p)
}

// TestBackpressureBlock Block 策略在积压满时阻塞发布，确认推进后自动放行；
// ctx 取消时以 ctx 错误返回而非死锁。
func TestBackpressureBlock(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 2
	cfg.BackpressurePolicy = BackpressureBlock
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("bp", 1); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("bp", "g", "c0")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, _, err := b.Publish(ctx, "bp", "k", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := b.Publish(context.Background(), "bp", "k", []byte("x"))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("publish should block until slot frees, returned %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	for i := 0; i < 2; i++ {
		ev, err := c.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked publish after ack: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked publish never released after ack")
	}
	t.Logf("判定依据: 积压满时发布阻塞，连续确认 2 个事件后阻塞发布成功返回；committed=%d",
		b.topics["bp"].partitions[0].committedOffset("g"))
}

// TestInvalidConfig 非法配置必须返回可区分的 ErrInvalidConfig。
func TestInvalidConfig(t *testing.T) {
	bad := testConfig(t.TempDir())
	bad.NumPartitions = 0
	if _, err := NewBroker(bad); !errorIs(err, ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	bad2 := testConfig(t.TempDir())
	bad2.Retry.MaxAttempts = 0
	if _, err := NewBroker(bad2); !errorIs(err, ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig for retry, got %v", err)
	}
	b, err := NewBroker(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.Subscribe("missing", "g", "c"); !errorIs(err, ErrUnknownTopic) {
		t.Fatalf("want ErrUnknownTopic, got %v", err)
	}
	if _, _, err := b.Publish(ctxBg(), "missing", "k", nil); !errorIs(err, ErrUnknownTopic) {
		t.Fatalf("want ErrUnknownTopic on publish, got %v", err)
	}
	if err := b.CreateTopic("tp", 1); err != nil {
		t.Fatal(err)
	}
	c2, err := b.Subscribe("tp", "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Ack(Event{Partition: 9, Offset: 0}); !errorIs(err, ErrUnknownPartition) {
		t.Fatalf("want ErrUnknownPartition, got %v", err)
	}
	t.Logf("判定依据: 非法配置=>ErrInvalidConfig；未知主题=>ErrUnknownTopic；未知分区=>ErrUnknownPartition")
}

func ctxBg() context.Context { return context.Background() }
