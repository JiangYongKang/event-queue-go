package eventqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func recTopic() TopicConfig {
	tc := defaultTopic()
	tc.Name = "rec"
	tc.PartitionNum = 1
	tc.Retry = RetryPolicy{
		MaxRetries:  10,
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		Multiplier:  2,
	}
	return tc
}

// errShutdown 模拟“崩溃中的 handler”在停机信号下退出，且绝不产生确认。
var errShutdown = errors.New("consumer shutting down without ack")

// closeBroker 在测试中显式关闭 broker（绕过 t.Cleanup 的二次 Close，幂等安全）。
func closeBroker(t *testing.T, b *Broker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestRestartRedelivery 停机时未确认的事件重启后必须重新投递；
// 已确认的事件重启后绝不再投递；分区顺序保持不变。
func TestRestartRedelivery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, recTopic())
	b := newTestBroker(t, cfg)

	// 第一轮：offset0 成功；offset1 正在处理（未返回=未确认）时直接停机，
	// 模拟处理中崩溃。handler 监听 broker 停机信号，在 Close 时主动退出阻塞，
	// 保证优雅关闭能在超时内完成，且 offset1 从未产生确认。
	delivered := make(chan *Event, 8)
	idem := NewIdempotentHandler()
	b.RegisterIdempotent(idem)
	h := idem.Wrap(func(ev *Event) error {
		delivered <- ev
		if ev.Offset == 1 {
			<-b.stopCh // 停机即退出，但此时不会再写 ack（handler 不再返回 nil 走确认）
			return errShutdown
		}
		return nil
	})
	c1, err := b.Subscribe("g1", "rec", h)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	evs := make([]*Event, 4)
	for i := 0; i < 4; i++ {
		evs[i], err = b.Publish(ctx, "rec", "k", []byte(fmt.Sprintf("m%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	e0 := <-delivered
	e1 := <-delivered // offset1 在途，直到停机都未确认
	// 等 offset0 落盘确认
	if !waitFor(time.Second, func() bool { return b.Stats().Acked == 1 }) {
		t.Fatal("offset0 未确认")
	}
	if err := c1.Ack(e0); errors.Is(err, ErrDuplicateAck) {
		// 自动确认已完成，重复确认符合预期
	} else if err != nil {
		t.Fatalf("ack e0: %v", err)
	}
	t.Logf("停机前位点：nextOffset=%d committed=1；offset1 在途未确认（事件ID=%s）",
		b.Stats().ByPartition["rec:0"].NextOffset, e1.ID)

	// 直接停机：handler 在停机信号下退出并返回 errShutdown，但 broker 已关闭，
	// finishAck 被拒绝（ErrBrokerClosed），磁盘上不存在 offset1 的 ack。
	closeBroker(t, b)

	// 重启：新 broker 必须回放磁盘；已确认的 offset0 不再投递，
	// 未确认的 offset1/2/3 按顺序重新投递。
	b2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b2.Close(ctx2)
	})

	var mu sync.Mutex
	var order []int64
	seen := map[int64]int{}
	idem2 := NewIdempotentHandler()
	b2.RegisterIdempotent(idem2)
	h2 := idem2.Wrap(func(ev *Event) error {
		mu.Lock()
		order = append(order, ev.Offset)
		seen[ev.Offset]++
		mu.Unlock()
		return nil
	})
	if _, err := b2.Subscribe("g1", "rec", h2); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool {
		st := b2.Stats()
		return st.Acked == 3 && st.Lag == 0
	}) {
		t.Fatalf("重启后未完成重投：%+v order=%v", b2.Stats(), order)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []int64{1, 2, 3}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("重启后重投顺序应为 %v（不含已确认的0），实际 %v", want, order)
	}
	for off, n := range seen {
		if n != 1 {
			t.Fatalf("offset %d 被投递 %d 次（期望恰好1次）", off, n)
		}
	}
	st := b2.Stats()
	t.Logf("恢复判定：重投序列=%v（offset0 已确认未再投递），redelivered=%d lag=%d",
		order, st.Redelivered, st.Lag)
}

// TestRestartRetryingEvent 失败退避中的事件在重启后必须重新投递并能成功，
// 尝试次数持久化延续语义由 at-least-once + 幂等保证（不丢、可重试）。
func TestRestartRetryingEvent(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, recTopic())
	b := newTestBroker(t, cfg)

	delivered := make(chan *Event, 2)
	h := func(ev *Event) error {
		delivered <- ev
		<-b.stopCh // 首次投递后处理未完成即停机，从不确认
		return errShutdown
	}
	if _, err := b.Subscribe("g1", "rec", h); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "rec", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	<-delivered
	closeBroker(t, b)

	// 重启后事件仍未确认，必须再次被投递
	b2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b2.Close(c)
	})
	if _, err := b2.Subscribe("g1", "rec", func(*Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool {
		st := b2.Stats()
		return st.Acked == 1 && st.Lag == 0
	}) {
		t.Fatalf("重启后失败未决事件未重投确认：%+v", b2.Stats())
	}
	t.Logf("恢复判定：停机时在途未确认事件重启后重新投递并成功，acked=1 lag=0")
}

// TestRestartDLQPersisted 死信记录重启后仍可查询，携带原因；位点不回退。
func TestRestartDLQPersisted(t *testing.T) {
	dir := t.TempDir()
	tc := recTopic()
	tc.Retry.MaxRetries = 1
	cfg := testConfig(t, dir, tc)
	b := newTestBroker(t, cfg)
	if _, err := b.Subscribe("g1", "rec", func(*Event) error {
		return errors.New("boom-persist")
	}); err != nil {
		t.Fatal(err)
	}
	ev, err := b.Publish(context.Background(), "rec", "k", []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.Stats().DeadLettered == 1 }) {
		t.Fatal("未进入死信")
	}
	closeBroker(t, b)

	b2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b2.Close(c)
	})
	rec, ok, err := b2.DLQByEvent(ev.ID)
	if err != nil || !ok {
		t.Fatalf("重启后死信记录丢失：ok=%v err=%v", ok, err)
	}
	if rec.Reason != "boom-persist" || rec.Attempts != 2 {
		t.Fatalf("死信记录不符：%+v", rec)
	}
	ps := b2.partitions[pKey{topic: "rec", partition: 0}].groups["g1"]
	if ps == nil || ps.committed != 1 {
		t.Fatalf("重启后 committed 应持久化为1，实际 %+v", ps)
	}
	t.Logf("恢复判定：死信持久化 id=%s reason=%q attempts=%d committed=1，事件不会被再次投递",
		rec.EventID, rec.Reason, rec.Attempts)

	// 同组重新订阅：已死信的 offset 不得再投递
	redelivered := make(chan int64, 4)
	if _, err := b2.Subscribe("g1", "rec", func(e *Event) error {
		redelivered <- e.Offset
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 再发一条新事件，确认它能正常投递，而旧 offset0 不会
	if _, err := b2.Publish(context.Background(), "rec", "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	select {
	case off := <-redelivered:
		if off != 1 {
			t.Fatalf("重启后只应投递新事件 offset1，实际投递了 offset %d", off)
		}
		t.Logf("关键判定：已死信 offset0 未再投递，新事件 offset1 正常")
	case <-time.After(2 * time.Second):
		t.Fatal("新事件未被投递")
	}
}

// TestRestartNewGroupConsumesHistory 重启后出现的新消费者组从最早留存事件开始消费，
// 且不影响老组位点（多组独立）。
func TestRestartNewGroupConsumesHistory(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, recTopic())
	b := newTestBroker(t, cfg)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := b.Publish(ctx, "rec", "k", []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// g1 全部确认
	if _, err := b.Subscribe("g1", "rec", func(*Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.Stats().Acked == 3 }) {
		t.Fatal("g1 未消费完")
	}
	closeBroker(t, b)

	// 重启后新组 g2：磁盘事件仍在（WAL 不做物理删除），应能从头消费历史
	b2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b2.Close(c)
	})
	var mu sync.Mutex
	var order []int64
	if _, err := b2.Subscribe("g2", "rec", func(ev *Event) error {
		mu.Lock()
		order = append(order, ev.Offset)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	}) {
		t.Fatalf("新组未消费到全部历史：%v", order)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(order) != "[0 1 2]" {
		t.Fatalf("新组历史消费顺序应为 [0 1 2]，实际 %v", order)
	}
	t.Logf("恢复判定：重启后新组 g2 从最早留存事件消费，顺序=%v", order)
}
