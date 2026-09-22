package eventqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPartitionOrdering 同一 key 的事件落在同一分区：
// 即使不同事件处理耗时不同，同一分区内被业务看到的顺序必须严格等于发布顺序。
func TestPartitionOrdering(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, defaultTopic()))

	var mu sync.Mutex
	seen := map[int][]int64{} // partition -> offsets in delivery order
	barrier := make(chan struct{})

	h := func(ev *Event) error {
		mu.Lock()
		seen[ev.Partition] = append(seen[ev.Partition], ev.Offset)
		mu.Unlock()
		// 故意让第一个分区上的 offset 0 阻塞，等确认 offset1 不会越过它后再放行。
		if ev.Partition == 0 && ev.Offset == 0 {
			select {
			case <-barrier:
			case <-time.After(2 * time.Second):
			}
		}
		if ev.Partition == 0 && ev.Offset == 1 {
			// offset1 若被错误提前投递，会在这里留时间让断言观察到。
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}
	if _, err := b.Subscribe("g1", "orders", h); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// 同一 key => 同一分区，发布 6 条
	for i := 0; i < 6; i++ {
		if _, err := b.Publish(ctx, "orders", "user-A", []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// 等待 offset0 正在处理（在途），offset1 此时绝不允许被业务看到
	time.Sleep(60 * time.Millisecond)
	mu.Lock()
	got := append([]int64(nil), seen[0]...)
	mu.Unlock()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("顺序判定：offset0 在途时，业务只能看到 [0]，实际看到 %v（出现越序投递）", got)
	}
	t.Logf("关键判定：offset0 在途期间已投递偏移=%v，队首阻塞生效，无越位", got)
	close(barrier)

	if !waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen[0]) == 6
	}) {
		t.Fatalf("未在超时前消费完，seen=%v", seen[0])
	}

	mu.Lock()
	order := seen[0]
	mu.Unlock()
	for i, off := range order {
		if off != int64(i) {
			t.Fatalf("乱序投递：分区0 实际业务可见顺序 %v，期望 [0 1 2 3 4 5]", order)
		}
	}
	t.Logf("关键位点：分区0 业务可见顺序=%v，严格等于发布顺序", order)

	st := b.Stats()
	if st.Lag != 0 {
		t.Fatalf("消费完成后积压应为0，实际 lag=%d", st.Lag)
	}
	t.Logf("指标：published=%d delivered=%d acked=%d redelivered=%d lag=%d",
		st.Published, st.Delivered, st.Acked, st.Redelivered, st.Lag)
}

// TestMultiPartitionParallel 不同分区必须能并行消费（并行度来自多分区）。
func TestMultiPartitionParallel(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, defaultTopic()))

	var mu sync.Mutex
	active, maxActive := 0, 0
	done := make(chan struct{})
	h := func(ev *Event) error {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		done <- struct{}{}
		return nil
	}
	if _, err := b.Subscribe("g1", "orders", h); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b.Publish(ctx, "orders", "k1", []byte("a")); err != nil { // part?
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "orders", "k2", []byte("b")); err != nil {
		t.Fatal(err)
	}
	// 若恰好同分区，补投直到两个分区都至少有1条
	for i := 3; i < 10; i++ {
		if _, err := b.Publish(ctx, "orders", fmt.Sprintf("k%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 9; i++ { // 发布了 9 条
		<-done
	}
	if maxActive < 2 {
		t.Fatalf("多分区并行判定：期望并发处理数>=2，实际峰值=%d", maxActive)
	}
	t.Logf("关键判定：跨分区并发处理峰值=%d（分区内仍严格串行）", maxActive)
}
