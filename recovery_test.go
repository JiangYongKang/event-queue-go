package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestRestartRecovery 发布 5 个事件，仅确认前 3 个（offset 0,1,2）后停机：
// 重启后 offset 0..2 不得再投递，offset 3,4 必须重新投递且成功；位点连续推进到 5。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()

	// ---- 第一次运行：消费前 3 个事件 ----
	cfg := testConfig(dir)
	b1, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic("rep", 1); err != nil {
		t.Fatal(err)
	}
	c1, err := b1.Subscribe("rep", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, err := b1.Publish(ctx, "rep", "k", []byte("e"+itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	acked := map[uint64]bool{}
	for len(acked) < 3 {
		ev, err := c1.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Offset > 2 {
			// offset 3 在 offset 0..2 全部确认前不会被派发（严格有序，窗口=1）。
			t.Fatalf("unexpected offset %d before first three acked", ev.Offset)
		}
		if err := c1.Ack(ev); err != nil {
			t.Fatal(err)
		}
		acked[ev.Offset] = true
	}
	if co := b1.topics["rep"].partitions[0].committedOffset("g"); co != 3 {
		t.Fatalf("before close committed=%d, want 3", co)
	}
	if err := b1.Close(); err != nil { // 优雅停机
		t.Fatal(err)
	}
	t.Logf("停机前: 已确认 offset 0,1,2；offset 3,4 未确认；committed=3 已持久化")

	// ---- 第二次运行：从同一目录恢复 ----
	b2, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	c2, err := b2.Subscribe("rep", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var redelivered []uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			ev, err := c2.Poll(ctx)
			if err != nil {
				return
			}
			if ev.Attempts != 1 {
				t.Errorf("recovered event %d attempts=%d, want 1 (fresh delivery after restart)", ev.Offset, ev.Attempts)
			}
			mu.Lock()
			redelivered = append(redelivered, ev.Offset)
			mu.Unlock()
			if err := c2.Ack(ev); err != nil {
				t.Errorf("Ack after restart offset=%d: %v", ev.Offset, err)
				return
			}
			if len(redelivered) == 2 {
				return
			}
		}
	}()
	waitFor(t, func() bool {
		mu.Lock()
		l := len(redelivered)
		mu.Unlock()
		return l == 2
	}, "recovered unacked events redelivered")
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(redelivered) != 2 || redelivered[0] != 3 || redelivered[1] != 4 {
		t.Fatalf("redelivered offsets=%v, want strictly [3 4]", redelivered)
	}
	waitFor(t, func() bool { return b2.Stats().Acked == 2 }, "acked after restart")
	if co := b2.topics["rep"].partitions[0].committedOffset("g"); co != 5 {
		t.Fatalf("committed after restart=%d, want 5", co)
	}
	if dl, _ := b2.DeadLetters("rep", "g"); len(dl) != 0 {
		t.Fatalf("unexpected dead letters: %d", len(dl))
	}
	t.Logf("判定依据: 重启后仅重新投递未确认的 [3 4]，已确认的 0..2 未再投递；committed=5；重启后本次 Stats=%+v",
		b2.Stats())
}

// TestRestartNoRedeliveryAfterAck 全部确认后重启：不得有任何事件再投递。
func TestRestartNoRedeliveryAfterAck(t *testing.T) {
	dir := t.TempDir()
	b1, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic("a", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c1, err := b1.Subscribe("a", "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go serve(ctx, b1, c1, Handler(func(ctx context.Context, e Event) error { return nil }), &wg, nil, nil)
	for i := 0; i < 10; i++ {
		if _, _, err := b1.Publish(ctx, "a", "k", nil); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return b1.Stats().Acked == 10 }, "all acked")
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	wg.Wait()

	b2, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	c2, err := b2.Subscribe("a", "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	// 恢复出的 committed=10，派发器没有任何事件可投；Poll 必须超时而非收到旧事件。
	pollCtx, pcancel := context.WithTimeout(context.Background(), 80*1e6)
	defer pcancel()
	if _, err := c2.Poll(pollCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded (no redelivery), got %v", err)
	}
	if co := b2.topics["a"].partitions[0].committedOffset("g"); co != 10 {
		t.Fatalf("committed restored=%d, want 10", co)
	}
	t.Logf("判定依据: 全部确认后重启，Poll 超时无投递；committed 从磁盘恢复为 10")
}

// TestRestartDLQPersistence 重试耗尽进入死信后重启：死信记录仍可查询。
func TestRestartDLQPersistence(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Retry.MaxAttempts = 1
	b1, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic("d", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c1, err := b1.Subscribe("d", "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go serve(ctx, b1, c1, Handler(func(ctx context.Context, e Event) error {
		return errors.New("permanent failure")
	}), &wg, nil, nil)
	if _, _, err := b1.Publish(ctx, "d", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return b1.Stats().DeadLettered == 1 }, "dead lettered")
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	wg.Wait()

	b2, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	dls, err := b2.DeadLetters("d", "g")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Event.Offset != 0 || dls[0].Attempts != 1 ||
		dls[0].Reason == "" {
		t.Fatalf("dls=%+v", dls)
	}
	// 死信是终态：重启后该 offset 不会再次投递（committed 已越过它）。
	if co := b2.topics["d"].partitions[0].committedOffset("g"); co != 1 {
		t.Fatalf("committed=%d, want 1", co)
	}
	if _, err := b2.Subscribe("d", "g", "c2"); err != nil {
		t.Fatal(err)
	}
	pollCtx, pcancel := context.WithTimeout(context.Background(), 80*1e6)
	defer pcancel()
	c2, _ := b2.Subscribe("d", "g", "c-poll")
	if _, err := c2.Poll(pollCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dead letter must not be redelivered, got %v", err)
	}
	t.Logf("判定依据: 重启后 DLQ 仍含 offset=0 reason=%q attempts=%d；committed=1，死信不重投",
		dls[0].Reason, dls[0].Attempts)
}

// TestRestartDuplicateAcceptedWithDedup 事件处理成功（副作用完成）但确认前停机，
// 重启后重新投递：幂等 Handler 跳过副作用、正常 Ack，并计入 Duplicates，而非失败。
func TestRestartDuplicateAcceptedWithDedup(t *testing.T) {
	dir := t.TempDir()

	// 第一次运行：在副作用执行后、Ack 之前模拟停机（in-flight 丢失）。
	b1, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic("dd", 1); err != nil {
		t.Fatal(err)
	}
	c1, err := b1.Subscribe("dd", "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := b1.Publish(ctx, "dd", "k", []byte("money")); err != nil {
		t.Fatal(err)
	}
	// 把去重存储模拟为副作用已落地的持久账本（实际系统中应持久化）。
	ledger := NewDedupStore()
	ev, err := c1.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 业务副作用完成并登记账本，但尚未 Ack 就停机。
	ledger.Claim(ev.ID)
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("停机前: 事件 %s 副作用已执行但未确认（committed=0）", ev.ID)

	// 第二次运行：事件被重新投递（至少一次语义）。
	b2, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	c2, err := b2.Subscribe("dd", "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	sideEffects := 0
	h := b2.NewDedupHandler(ledger, Handler(func(ctx context.Context, e Event) error {
		sideEffects++ // 绝不应被再次执行
		return nil
	}))
	ev2, err := c2.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ev2.ID != ev.ID || ev2.Offset != 0 {
		t.Fatalf("redelivered event mismatch: %+v", ev2)
	}
	if err := h(ctx, ev2); err != nil {
		t.Fatalf("duplicate delivery must be accepted, got %v", err)
	}
	if err := c2.Ack(ev2); err != nil {
		t.Fatal(err)
	}
	if sideEffects != 0 {
		t.Fatalf("duplicate caused %d repeated side effects, want 0", sideEffects)
	}
	if b2.Stats().Duplicates != 1 {
		t.Fatalf("Duplicates=%d, want 1", b2.Stats().Duplicates)
	}
	if b2.Stats().DeadLettered != 0 || b2.Stats().Acked != 1 {
		t.Fatalf("stats=%+v", b2.Stats())
	}
	t.Logf("判定依据: 重投事件被幂等账本命中 => 副作用执行 0 次、Duplicates=1、正常 Ack；Stats=%+v", b2.Stats())
}
