package queue

import (
	"context"
	"testing"
	"time"
)

// drain 轮询并确认 n 个事件，断言取到的位点严格等于 0..n-1 且 Attempts 均为 1。
// 返回取到的事件顺序，便于调用方继续断言（历史事件不丢、不乱序）。
func drain(t *testing.T, c *Consumer, n int, ctx context.Context) []uint64 {
	t.Helper()
	var seq []uint64
	for i := 0; i < n; i++ {
		ev, err := c.Poll(ctx)
		if err != nil {
			t.Fatalf("Poll %d: %v", i, err)
		}
		if ev.Offset != uint64(i) {
			t.Fatalf("history order violated at #%d: got offset %d, want %d; seq=%v", i, ev.Offset, i, seq)
		}
		if ev.Attempts != 1 {
			t.Fatalf("offset %d attempts=%d, want 1 (fresh historical delivery)", ev.Offset, ev.Attempts)
		}
		seq = append(seq, ev.Offset)
		if err := c.Ack(ev); err != nil {
			t.Fatalf("Ack offset=%d: %v", ev.Offset, err)
		}
	}
	return seq
}

// TestPublishBeforeSubscribe 生产端先启动、先写入事件，消费组很久之后才订阅：
// 历史事件必须按分区顺序从头全部消费到，不丢、不乱序；
// 且没有订阅者期间，积压指标如实反映容量占用。
func TestPublishBeforeSubscribe(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 0 // 不限流，允许历史积压
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("late", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	const n = 100
	for i := 0; i < n; i++ {
		if _, off, err := b.Publish(ctx, "late", "k", []byte("p"+itoa(i))); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		} else if off != uint64(i) {
			t.Fatalf("offset=%d, want %d", off, i)
		}
	}

	// 没有任何消费组订阅：积压仍按整个保留窗口如实计数，容量判定不会“因无人订阅而失效”。
	m := b.Backlog()[0]
	if m.Pending != n || m.PayloadBytes <= 0 || m.InFlight != 0 {
		t.Fatalf("no-subscriber metrics=%+v, want Pending=%d Bytes>0 InFlight=0", m, n)
	}
	t.Logf("无订阅者: Pending=%d PayloadBytes=%d（容量口径不依赖订阅者存在）Stats=%+v",
		m.Pending, m.PayloadBytes, b.Stats())

	// 消费端晚到：从头按序消费全部历史。
	c, err := b.Subscribe("late", "g-late", "w1")
	if err != nil {
		t.Fatal(err)
	}
	seq := drain(t, c, n, ctx)
	if len(seq) != n || seq[0] != 0 || seq[n-1] != n-1 {
		t.Fatalf("history seq len=%d head=%d tail=%d", len(seq), seq[0], seq[n-1])
	}
	p := b.topics["late"].partitions[0]
	waitFor(t, func() bool { return b.Stats().Acked == n }, "all history acked")
	base, head := p.retentionWindow()
	t.Logf("消费端晚到消费完成: seq=0..%d 严格有序；committed=%d 保留窗口=[%d,%d) Stats=%+v",
		n-1, p.committedOffset("g-late"), base, head, b.Stats())
	if co := p.committedOffset("g-late"); co != n {
		t.Fatalf("committed=%d, want %d", co, n)
	}
}

// TestHistoryDeliveredFromDiskAfterRestart 无订阅者期间发布并重启：
// 历史事件体不会随恢复进入内存，但订阅后必须经“缺页读盘”路径按序投递、
// 位点推进与平时完全一致。
func TestHistoryDeliveredFromDiskAfterRestart(t *testing.T) {
	dir := t.TempDir()

	b1, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic("cold", 1); err != nil {
		t.Fatal(err)
	}
	const n = 70 // 大于缺页块 64，至少触发一次读盘分页
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, _, err := b1.Publish(ctx, "cold", "k", []byte{byte(i)}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("停机前: %d 个事件已落盘，从未有任何消费组订阅", n)

	b2, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	p := b2.topics["cold"].partitions[0]

	// 恢复后：计数与积压如实，事件体 0 驻留（常驻内存不随历史量增长）。
	if got := b2.Stats().Published; got != n {
		t.Fatalf("restored Published=%d, want %d", got, n)
	}
	if m := b2.Backlog()[0]; m.Pending != n || m.PayloadBytes != n {
		t.Fatalf("restored metrics=%+v, want Pending=%d Bytes=%d", m, n, n)
	}
	if r := p.residentEvents(); r != 0 {
		t.Fatalf("resident events after restart=%d, want 0 (history paged on demand)", r)
	}
	base, head := p.retentionWindow()
	t.Logf("重启后未订阅: 保留窗口=[%d,%d) Pending=%d 驻留事件体=%d（历史只在磁盘）",
		base, head, head-base, p.residentEvents())

	c, err := b2.Subscribe("cold", "g-cold", "w1")
	if err != nil {
		t.Fatal(err)
	}
	drain(t, c, n, ctx)
	waitFor(t, func() bool { return b2.Stats().Acked == n }, "cold history acked")
	if co := p.committedOffset("g-cold"); co != n {
		t.Fatalf("committed=%d, want %d", co, n)
	}
	t.Logf("缺页读盘投递完成: 历史 0..%d 按序消费、位点推进到 %d；驻留事件体=%d Stats=%+v",
		n-1, p.committedOffset("g-cold"), p.residentEvents(), b2.Stats())
}

// TestBackpressureWithoutConsumers 完全没有消费者的阶段把分区发到接近和超过上限：
// Reject 策略照既定策略拒绝并计数，指标如实；确认腾出空间后可继续发布。
func TestBackpressureWithoutConsumers(t *testing.T) {
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
	ctx := context.Background()

	// 接近上限：3 条全部成功（没有任何订阅者）。
	for i := 0; i < 3; i++ {
		if _, _, err := b.Publish(ctx, "bp", "k", []byte("xy")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if m := b.Backlog()[0]; m.Pending != 3 || m.PayloadBytes != 6 {
		t.Fatalf("at-limit metrics=%+v, want Pending=3 Bytes=6", m)
	}
	// 超过上限：立即拒绝，而不是因没有订阅者而无限放行。
	if _, _, err := b.Publish(ctx, "bp", "k", []byte("xy")); !errorIs(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure with no consumer, got %v", err)
	}
	if got := b.Stats().Rejected; got != 1 {
		t.Fatalf("Rejected=%d, want 1", got)
	}
	t.Logf("无消费者超上限: 容量判定照常生效，第 4 次发布 ErrBackpressure；指标=%+v",
		b.Backlog()[0])

	// 字节上限同样在无消费者时生效。
	cfg2 := testConfig(t.TempDir())
	cfg2.MaxMemoryBytes = 5
	cfg2.BackpressurePolicy = BackpressureReject
	b2, err := NewBroker(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("bp2", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b2.Publish(ctx, "bp2", "k", []byte("12345")); err != nil {
		t.Fatalf("publish to byte limit: %v", err)
	}
	if _, _, err := b2.Publish(ctx, "bp2", "k", []byte("1")); !errorIs(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure on byte limit, got %v", err)
	}
	if m := b2.Backlog()[0]; m.PayloadBytes != 5 {
		t.Fatalf("byte metrics=%+v, want Bytes=5", m)
	}
	t.Logf("无消费者字节上限: PayloadBytes=%d 达到 MaxMemoryBytes=5，下一条被拒绝",
		b2.Backlog()[0].PayloadBytes)

	// 消费者晚到、确认一个后，按条数腾出一个名额，发布恢复。
	c, err := b.Subscribe("bp", "g", "w1")
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
	waitFor(t, func() bool {
		_, _, e := publishNow(b, "bp", "k", []byte("xy"))
		return e == nil
	}, "publish succeeds after late consumer acks")
	if m := b.Backlog()[0]; m.Pending != 3 {
		t.Fatalf("Pending after refill=%d, want 3", m.Pending)
	}
	t.Logf("晚到消费确认后: committed=%d，发布解除限制，Pending 回到上限 3",
		b.topics["bp"].partitions[0].committedOffset("g"))
}

// TestBlockWithoutConsumersReleasedOnLateSubscribe Block 策略下无消费者时发布会阻塞，
// 消费组晚到并确认后放行；全程不死锁。
func TestBlockWithoutConsumersReleasedOnLateSubscribe(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 1
	cfg.BackpressurePolicy = BackpressureBlock
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("bp", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Publish(context.Background(), "bp", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := b.Publish(context.Background(), "bp", "k", []byte("x"))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("publish should block without consumer, got %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	c, err := b.Subscribe("bp", "g", "w1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
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
			t.Fatalf("blocked publish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked publish never released after late subscribe + ack")
	}
	t.Logf("判定依据: 无消费者时发布阻塞；晚到消费组连续确认后阻塞发布放行，committed=%d",
		b.topics["bp"].partitions[0].committedOffset("g"))
}

// TestMemoryBoundedOverLongRun 累计发布量远大于上限时，常驻内存只与上限相关：
// 已被所有组确认的旧事件淘汰出内存，且不影响重启恢复、不重投已确认事件、
// 不打乱晚到组的位点与顺序。
func TestMemoryBoundedOverLongRun(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxBacklogPerPartition = 8 // 很小的积压上限
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("run", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := b.Subscribe("run", "g1", "w1")
	if err != nil {
		t.Fatal(err)
	}
	// 以小批量“发 4 条、消费 4 条”推进：积压始终 <= 4（低于上限 8），
	// 确认后旧事件被持续淘汰，以此验证常驻内存只随上限、不随累计发布量增长。
	residentPeak := 0
	const total = 300
	for batch := 0; batch*4 < total; batch++ {
		k := 4
		if total-batch*4 < 4 {
			k = total - batch*4
		}
		for j := 0; j < k; j++ {
			if _, _, err := b.Publish(ctx, "run", "k", []byte("payload")); err != nil {
				t.Fatalf("Publish batch %d: %v", batch, err)
			}
		}
		if r := b.topics["run"].partitions[0].residentEvents(); r > residentPeak {
			residentPeak = r
		}
		for j := 0; j < k; j++ {
			ev, perr := c.Poll(ctx)
			if perr != nil {
				t.Fatal(perr)
			}
			if err := c.Ack(ev); err != nil {
				t.Fatal(err)
			}
		}
	}

	p := b.topics["run"].partitions[0]
	waitFor(t, func() bool { return b.Stats().Acked == total }, "all long-run acked")
	base, head := p.retentionWindow()
	if head != total || base != total {
		t.Fatalf("window=[%d,%d), want [%d,%d) (all confirmed evicted)", base, head, total, total)
	}
	if r := p.residentEvents(); r != 0 {
		t.Fatalf("resident events after catch-up=%d, want 0 (memory tracks limit, not cumulative)", r)
	}
	t.Logf("长跑: 累计发布=%d 远超上限=8；驻留峰值≈%d；追平后窗口=[%d,%d) 驻留=%d",
		total, residentPeak, base, head, p.residentEvents())

	// 新消费组此时才订阅：已确认的旧事件绝不重投，只收到订阅之后的新事件。
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	cLate, err := b2.Subscribe("run", "g-late", "w")
	if err != nil {
		t.Fatal(err)
	}
	p2 := b2.topics["run"].partitions[0]
	b2base, b2head := p2.retentionWindow()
	if b2base != total || b2head != total || p2.residentEvents() != 0 {
		t.Fatalf("restart window=[%d,%d) resident=%d", b2base, b2head, p2.residentEvents())
	}
	pollCtx, pcancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer pcancel()
	if _, err := cLate.Poll(pollCtx); !errorIs(err, context.DeadlineExceeded) {
		t.Fatalf("late group must not receive confirmed history, got %v", err)
	}
	// 晚到组订阅后发布的新事件必须正常、按序收到。
	if _, _, err := b2.Publish(context.Background(), "run", "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	ev, err := cLate.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ev.Offset != total {
		t.Fatalf("late group got offset=%d, want %d (no replay of confirmed history)", ev.Offset, total)
	}
	if err := cLate.Ack(ev); err != nil {
		t.Fatal(err)
	}
	t.Logf("重启+晚到组: 已确认旧事件 0 重投；新事件位点=%d 正常投递，位点/顺序语义不变", total)
}
