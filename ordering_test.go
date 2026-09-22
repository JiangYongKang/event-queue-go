package queue

import (
	"context"
	"sync"
	"testing"
)

// TestInOrderDelivery 三个消费者并行消费同一分区：
// 断言事件严格按发布顺序被取走与确认，无丢失、无乱序。
func TestInOrderDelivery(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer b.Close()
	if err := b.CreateTopic("orders", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	const n = 50
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	got := map[int]int{} // 每个消费者取到的事件数
	var seq []uint64     // 全局取走顺序
	var errs []error

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		c, err := b.Subscribe("orders", "g1", "c"+itoa(i))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		wg.Add(1)
		ci := i
		go serve(ctx, b, c, func(ctx context.Context, e Event) error {
			mu.Lock()
			seq = append(seq, e.Offset)
			got[ci]++
			mu.Unlock()
			return nil
		}, &wg, &errs, &mu)
	}

	// 全部事件使用同一 Key => 落在同一分区。
	for i := 0; i < n; i++ {
		pi, off, err := b.Publish(ctx, "orders", "same-key", []byte("p"+itoa(i)))
		if err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		if pi != 0 || int(off) != i {
			t.Fatalf("offset mismatch: pi=%d off=%d want 0,%d", pi, off, i)
		}
	}

	waitFor(t, func() bool {
		st := b.Stats()
		return st.Acked == uint64(n)
	}, "all events acked")

	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seq) != n {
		t.Fatalf("delivered %d, want %d (loss)", len(seq), n)
	}
	for i, off := range seq {
		if off != uint64(i) {
			t.Fatalf("delivery order violated at #%d: got offset %d, want %d; seq=%v", i, off, i, seq)
		}
	}
	t.Logf("判定依据: 全局取走顺序严格递增 seq[0..%d]=0..%d；各消费者取到数量=%v（总和=%d）",
		n-1, n-1, got, len(seq))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if co := b.topics["orders"].partitions[0].committedOffset("g1"); co != n {
		t.Fatalf("committed offset = %d, want %d", co, n)
	}
	t.Logf("关键位点: g1 committed=%d, Stats=%+v",
		b.topics["orders"].partitions[0].committedOffset("g1"), b.Stats())
}

// TestAckErrors 验证确认异常链路可区分：未知事件 / 重复确认 / 位点回退。
func TestAckErrors(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	c, err := b.Subscribe("t", "g", "c0")
	if err != nil {
		t.Fatal(err)
	}
	p := b.topics["t"].partitions[0]

	// 未知事件（尚未发布 offset 3）。
	if err := c.Ack(Event{Partition: 0, Offset: 3, Attempts: 1}); !errorIs(err, ErrUnknownEvent) {
		t.Fatalf("want ErrUnknownEvent, got %v", err)
	}

	go func() {
		if _, _, err := b.Publish(ctx, "t", "k", []byte("x")); err != nil {
			t.Errorf("Publish: %v", err)
		}
	}()
	ev, err := c.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 位点回退：用旧版本号确认。
	old := ev
	old.Attempts = ev.Attempts - 1
	if err := c.Ack(old); !errorIs(err, ErrOffsetRegression) {
		t.Fatalf("want ErrOffsetRegression, got %v", err)
	}

	// 正常确认。
	if err := c.Ack(ev); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// 重复确认必须明确拒绝。
	if err := c.Ack(ev); !errorIs(err, ErrDuplicateAck) {
		t.Fatalf("want ErrDuplicateAck, got %v", err)
	}
	t.Logf("判定依据: 未投递确认=>ErrUnknownEvent；旧版本确认=>ErrOffsetRegression；二次确认=>ErrDuplicateAck；committed=%d",
		p.committedOffset("g"))
}
