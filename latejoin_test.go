package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestLateSubscriberConsumesHistoryInOrder 先发布、后订阅：
// 没有任何消费组时发布的事件全部可靠保留；消费组上线后从位点 0 开始
// 按分区顺序把历史事件全部消费到，不丢、不乱序，位点推进与平时一致。
func TestLateSubscriberConsumesHistoryInOrder(t *testing.T) {
	b, err := NewBroker(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("late", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	const n = 20
	for i := 0; i < n; i++ {
		if _, _, err := b.Publish(ctx, "late", "k", []byte(fmt.Sprintf("h%02d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	// 没有订阅者时积压指标也必须如实反映：20 条全部待消费。
	if m := b.Backlog()[0]; m.Pending != n {
		t.Fatalf("no-subscriber backlog=%+v, want Pending=%d", m, n)
	}
	t.Logf("订阅前: 已发布 %d 条，无消费组；Backlog=%+v", n, b.Backlog()[0])

	c, err := b.Subscribe("late", "g1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	dedup := NewDedupStore()
	sideEffects := 0
	h := b.NewDedupHandler(dedup, Handler(func(ctx context.Context, e Event) error {
		sideEffects++
		return nil
	}))
	for i := 0; i < n; i++ {
		ev, err := c.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset != uint64(i) || ev.Payload == nil || string(ev.Payload) != fmt.Sprintf("h%02d", i) {
			t.Fatalf("out of order at %d: got offset=%d payload=%q", i, ev.Offset, ev.Payload)
		}
		if ev.Attempts != 1 {
			t.Fatalf("offset %d attempts=%d, want 1", ev.Offset, ev.Attempts)
		}
		if err := h(ctx, ev); err != nil {
			t.Fatal(err)
		}
		if err := c.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	p := b.topics["late"].partitions[0]
	if co := p.committedOffset("g1"); co != n {
		t.Fatalf("committed=%d, want %d", co, n)
	}
	if sideEffects != n {
		t.Fatalf("side effects=%d, want %d", sideEffects, n)
	}
	if s := b.Stats(); s.Delivered != n || s.Acked != n || s.Duplicates != 0 {
		t.Fatalf("stats=%+v", s)
	}
	t.Logf("判定依据: 晚到消费组按序消费 [0..%d]，payload 全对；committed=%d；副作用=%d 次；Stats=%+v",
		n-1, p.committedOffset("g1"), sideEffects, b.Stats())
}

// TestLateSubscriberReadsEvictedHistoryFromDisk 历史事件已因容量控制逐出内存
// （被所有已知消费组确认）后，全新消费组订阅：必须能从磁盘日志回读这段历史，
// 按序投递、位点正常推进，语义与内存命中完全一致。
func TestLateSubscriberReadsEvictedHistoryFromDisk(t *testing.T) {
	b, err := NewBroker(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("ev", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := b.topics["ev"].partitions[0]

	const n = 30
	early, err := b.Subscribe("ev", "early", "c0")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, _, err := b.Publish(ctx, "ev", "k", []byte(fmt.Sprintf("p%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		ev, err := early.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := early.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return p.committedOffset("early") == n }, "early group caught up")

	// 容量控制生效：全部被确认后内存窗口应已腾空，事件只留在磁盘。
	base, win := p.memWindow()
	if base != n || win != 0 {
		t.Fatalf("memWindow base=%d len=%d, want base=%d len=0", base, win, n)
	}
	if m := b.Backlog()[0]; m.Pending != 0 || m.PayloadBytes != 0 {
		t.Fatalf("backlog after full ack=%+v, want 0", m)
	}
	t.Logf("逐出后: committed=%d，内存窗口 base=%d len=%d（历史仅在磁盘）", p.committedOffset("early"), base, win)

	// 全新消费组晚到：从位点 0 回读磁盘历史，按序投递。
	late, err := b.Subscribe("ev", "late", "c9")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ev, err := late.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset != uint64(i) || string(ev.Payload) != fmt.Sprintf("p%02d", i) {
			t.Fatalf("out of order at %d: got offset=%d payload=%q", i, ev.Offset, ev.Payload)
		}
		if ev.Attempts != 1 {
			t.Fatalf("offset %d attempts=%d, want 1 (disk read-back is not a retry)", ev.Offset, ev.Attempts)
		}
		if err := late.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	if co := p.committedOffset("late"); co != n {
		t.Fatalf("late committed=%d, want %d", co, n)
	}
	if s := b.Stats(); s.Duplicates != 0 || s.DeadLettered != 0 {
		t.Fatalf("stats=%+v", s)
	}
	t.Logf("判定依据: 晚到组从磁盘回读 [0..%d] 严格有序、Attempts 全为 1；committed=%d；Stats=%+v",
		n-1, p.committedOffset("late"), b.Stats())
}

// TestNoConsumerBackpressureReject 完全没有消费者时，分区积压仍受上限约束：
// Reject 策略超限即拒绝并计数，积压与容量指标如实反映，不因没有订阅者而失效。
func TestNoConsumerBackpressureReject(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 3
	cfg.BackpressurePolicy = BackpressureReject
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("nc", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 没有任何订阅者：前 3 条成功，第 4 条必须被拒绝。
	for i := 0; i < 3; i++ {
		if _, _, err := b.Publish(ctx, "nc", "k", []byte("x")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if _, _, err := b.Publish(ctx, "nc", "k", []byte("x")); !errorIs(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure with no consumers, got %v", err)
	}
	if b.Stats().Rejected != 1 {
		t.Fatalf("Rejected=%d, want 1", b.Stats().Rejected)
	}
	m := b.Backlog()[0]
	if m.Pending != 3 || m.PayloadBytes != 3 {
		t.Fatalf("metrics=%+v, want Pending=3 Bytes=3", m)
	}
	t.Logf("无消费者阶段: 上限=3，第 4 次发布被拒绝；Backlog=%+v Rejected=%d", m, b.Stats().Rejected)

	// 消费组晚上线：先确认一条腾出空间，发布随即恢复；历史一条不丢。
	c, err := b.Subscribe("nc", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := c.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Offset != 0 {
		t.Fatalf("first offset=%d, want 0", ev.Offset)
	}
	if err := c.Ack(ev); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, _, e := publishNow(b, "nc", "k", []byte("x"))
		return e == nil
	}, "publish succeeds after late consumer acks")
	got := []uint64{0}
	for len(got) < 4 {
		ev, err := c.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ev.Offset)
		if err := c.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	want := []uint64{0, 1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered offsets=%v, want %v（历史按序不丢）", got, want)
		}
	}
	t.Logf("判定依据: 晚到消费者确认后发布恢复；投递位点=%v 严格有序；committed=%d",
		got, b.topics["nc"].partitions[0].committedOffset("g"))
}

// TestNoConsumerBackpressureBlock 没有消费者时 Block 策略阻塞发布；
// 消费组晚上线并确认后，阻塞的发布自动放行。
func TestNoConsumerBackpressureBlock(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 2
	cfg.BackpressurePolicy = BackpressureBlock
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("nb", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, _, err := b.Publish(ctx, "nb", "k", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := b.Publish(context.Background(), "nb", "k", []byte("x"))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("publish must block with no consumers and full backlog, returned %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	t.Logf("无消费者阶段: 积压=2 达到上限，第 3 次发布阻塞中；Backlog=%+v", b.Backlog()[0])

	c, err := b.Subscribe("nb", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	ev, err := c.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ev); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked publish after late consumer ack: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked publish never released after late consumer ack")
	}
	t.Logf("判定依据: 晚到消费者确认 1 条后阻塞发布放行；committed=%d",
		b.topics["nb"].partitions[0].committedOffset("g"))
}

// TestNoConsumerMemoryBytesCap 没有消费者时内存（payload 字节）上限同样生效，
// 容量判断不依赖订阅者在场。
func TestNoConsumerMemoryBytesCap(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxMemoryBytes = 8
	cfg.BackpressurePolicy = BackpressureReject
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("mc", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, _, err := b.Publish(ctx, "mc", "k", []byte("xxxx")); err != nil { // 各 4 字节
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if _, _, err := b.Publish(ctx, "mc", "k", []byte("xxxx")); !errorIs(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure at byte cap, got %v", err)
	}
	m := b.Backlog()[0]
	if m.Pending != 2 || m.PayloadBytes != 8 {
		t.Fatalf("metrics=%+v, want Pending=2 Bytes=8", m)
	}
	t.Logf("判定依据: 字节上限=8，两条 4 字节事件后第 3 条被拒绝；Backlog=%+v", m)
}

// TestMemoryBoundedUnderSustainedLoad 累计发布量远大于配置上限时：
// 运行中常驻内存只与上限相关（已确认前缀持续逐出）；重启后已确认事件不重投、
// 内存窗口不放大；全新消费组仍能从磁盘读回全部历史。
func TestMemoryBoundedUnderSustainedLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBacklogPerPartition = 5
	cfg.BackpressurePolicy = BackpressureBlock

	b1, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic("mm", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c1, err := b1.Subscribe("mm", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go serve(ctx, b1, c1, Handler(func(ctx context.Context, e Event) error { return nil }), &wg, nil, nil)

	const n = 100 // 20 倍于积压上限
	for i := 0; i < n; i++ {
		if _, _, err := b1.Publish(ctx, "mm", "k", []byte(fmt.Sprintf("m%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return b1.Stats().Acked == n }, "all acked")
	p1 := b1.topics["mm"].partitions[0]
	base, win := p1.memWindow()
	if base != n || win != 0 {
		t.Fatalf("memWindow base=%d len=%d after %d publishes, want base=%d len=0", base, win, n, n)
	}
	t.Logf("运行中: 累计发布 %d（上限 5），全部确认后内存窗口 base=%d len=%d（不随累计量增长）", n, base, win)
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	wg.Wait()

	// 重启：已确认事件不重投，启动即按确认水位逐出，内存不放大。
	b2, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	p2 := b2.topics["mm"].partitions[0]
	base, win = p2.memWindow()
	if base != n || win != 0 {
		t.Fatalf("after restart memWindow base=%d len=%d, want base=%d len=0", base, win, n)
	}
	c2, err := b2.Subscribe("mm", "g", "c2")
	if err != nil {
		t.Fatal(err)
	}
	pollCtx, pcancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer pcancel()
	if _, err := c2.Poll(pollCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("confirmed events must not be redelivered after restart, got %v", err)
	}
	t.Logf("重启后: 内存窗口 base=%d len=%d；已确认组 Poll 超时无重投；committed=%d",
		base, win, p2.committedOffset("g"))

	// 重启后全新消费组：从磁盘读回全部 100 条历史，严格有序。
	c3, err := b2.Subscribe("mm", "g2", "c3")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ev, err := c3.Poll(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset != uint64(i) || string(ev.Payload) != fmt.Sprintf("m%03d", i) {
			t.Fatalf("out of order at %d: got offset=%d payload=%q", i, ev.Offset, ev.Payload)
		}
		if err := c3.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	if co := p2.committedOffset("g2"); co != n {
		t.Fatalf("g2 committed=%d, want %d", co, n)
	}
	t.Logf("判定依据: 重启后新组从磁盘按序读回 [0..%d]；g2 committed=%d；Stats=%+v",
		n-1, p2.committedOffset("g2"), b2.Stats())
}
