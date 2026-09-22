package eventqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func retryTopic(maxRetries int) TopicConfig {
	tc := defaultTopic()
	tc.Name = "jobs"
	tc.PartitionNum = 1
	tc.Retry = RetryPolicy{
		MaxRetries:  maxRetries,
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		Multiplier:  2,
	}
	return tc
}

// TestRetryThenSuccess 前两次失败、第三次成功：必须重试且最终确认，不乱序、不丢。
func TestRetryThenSuccess(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, retryTopic(3)))
	idem := NewIdempotentHandler()
	b.RegisterIdempotent(idem)

	var calls atomic.Int64
	var mu sync.Mutex
	var order []int64
	h := idem.Wrap(func(ev *Event) error {
		n := calls.Add(1)
		mu.Lock()
		order = append(order, ev.Offset)
		mu.Unlock()
		if n < 3 {
			return fmt.Errorf("transient-%d", n)
		}
		return nil
	})
	if _, err := b.Subscribe("g1", "jobs", h); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b.Publish(ctx, "jobs", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool {
		st := b.Stats()
		return st.Acked == 1 && st.Lag == 0
	}) {
		st := b.Stats()
		t.Fatalf("重试后未确认：calls=%d stats=%+v", calls.Load(), st)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("调用次数判定：期望 3（首投+2次重试），实际 %d", got)
	}
	t.Logf("关键判定：业务调用 %d 次后成功；投递序列=%v（同 offset 重复投递为允许的至少一次语义）",
		calls.Load(), order)
	st := b.Stats()
	t.Logf("指标 delivered=%d redelivered=%d acked=%d failed=%d dead=%d",
		st.Delivered, st.Redelivered, st.Acked, st.Failed, st.DeadLettered)
	if st.DeadLettered != 0 {
		t.Fatalf("不应进入死信，实际 dead=%d", st.DeadLettered)
	}
}

// TestRetryExhaustedDLQ 超过最大重试次数必须进入死信，携带原因，位点继续推进。
func TestRetryExhaustedDLQ(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, retryTopic(2))) // 首投+2次重试=3次
	var calls atomic.Int64
	var mu sync.Mutex
	var order []int64
	var failErr = errors.New("permanent-boom")
	h := func(ev *Event) error {
		calls.Add(1)
		mu.Lock()
		order = append(order, ev.Offset)
		mu.Unlock()
		if string(ev.Payload) == "poison" {
			return failErr
		}
		return nil // 后续事件必须能越过死信位点继续消费
	}
	if _, err := b.Subscribe("g1", "jobs", h); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var poison, after *Event
	if poison, _ = b.Publish(ctx, "jobs", "k", []byte("poison")); poison == nil {
		t.Fatal("publish poison")
	}
	if after, _ = b.Publish(ctx, "jobs", "k", []byte("after")); after == nil {
		t.Fatal("publish after")
	}

	// 死信事件被终结后，同分区后续事件必须能够继续被投递（死信不永久卡住分区）
	if !waitFor(2*time.Second, func() bool {
		st := b.Stats()
		return st.DeadLettered == 1 && st.Acked == 1 && st.Lag == 0
	}) {
		t.Fatalf("死信/推进未达成：calls=%d stats=%+v order=%v", calls.Load(), b.Stats(), order)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("调用次数判定：poison 3 次 + after 1 次 = 4，实际 %d", got)
	}
	mu.Lock()
	t.Logf("关键位点：业务可见投递序列=%v（poison=offset0 试3次，after=offset1 试1次）", order)
	mu.Unlock()

	dlq := b.DLQ()
	if len(dlq) != 1 {
		t.Fatalf("死信条数应为1，实际 %d", len(dlq))
	}
	r := dlq[0]
	if r.EventID != poison.ID || r.Offset != 0 || r.Attempts != 3 {
		t.Fatalf("死信记录不符：%+v", r)
	}
	if r.Reason != "permanent-boom" {
		t.Fatalf("死信必须携带失败原因，实际 %q", r.Reason)
	}
	t.Logf("死信判定：id=%s offset=%d attempts=%d reason=%q deadAt=%s",
		r.EventID, r.Offset, r.Attempts, r.Reason, r.DeadAt.Format(time.RFC3339Nano))

	if got, ok, _ := b.DLQByEvent(poison.ID); !ok || got.Reason != "permanent-boom" {
		t.Fatalf("按事件ID查询死信失败：%+v ok=%v", got, ok)
	}
	if _, ok, _ := b.DLQByEvent(after.ID); ok {
		t.Fatal("成功事件不应出现在死信中")
	}
	st := b.Stats()
	if st.DeadLettered != 1 {
		t.Fatalf("死信计数应为1，实际 %d", st.DeadLettered)
	}
}

// TestRedeliveryAcceptedAsDuplicate 重启/重试导致的重复投递必须被正常接受、
// 计入 DuplicateCalls，而不是当作失败。
func TestRedeliveryAcceptedAsDuplicate(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, retryTopic(5))
	b := newTestBroker(t, cfg)
	idem := NewIdempotentHandler()
	b.RegisterIdempotent(idem)

	var calls atomic.Int64
	// 前两次失败触发重投；幂等包装器只在成功时记忆，所以前两次业务仍执行，
	// 成功后若再收到同一事件（模拟重启重投），直接接受为重复。
	h := idem.Wrap(func(ev *Event) error {
		n := calls.Add(1)
		if n < 3 {
			return errors.New("fail-then-retry")
		}
		return nil
	})
	if _, err := b.Subscribe("g1", "jobs", h); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ev, err := b.Publish(ctx, "jobs", "k", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.Stats().Acked == 1 }) {
		t.Fatal("未确认")
	}
	// 重启后旧事件不会重投（已确认）。这里直接再调用一次包装器模拟“重复投递”，
	// 验证重复被正常接受（nil），并计入 DuplicateCalls 而非失败。
	if err := idem.Wrap(func(*Event) error {
		t.Fatal("重复事件不得再次执行业务副作用")
		return nil
	})(ev); err != nil {
		t.Fatalf("重复投递必须被正常接受（nil），实际返回错误：%v", err)
	}
	if idem.DuplicateCalls() != 1 {
		t.Fatalf("重复投递计数应为1，实际 %d", idem.DuplicateCalls())
	}
	st := b.Stats()
	if st.DuplicateCalls != 1 {
		t.Fatalf("Broker 统计中的重复投递应为1，实际 %d", st.DuplicateCalls)
	}
	t.Logf("关键判定：重复投递被接受且计入 DuplicateCalls=%d，未计入失败 failed=%d",
		st.DuplicateCalls, st.Failed)
}

// TestNoInfiniteRetry 重试次数有界：事件投递次数恰好为 MaxRetries+1。
func TestNoInfiniteRetry(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, retryTopic(1))) // 仅 1 次重试
	var calls atomic.Int64
	if _, err := b.Subscribe("g1", "jobs", func(*Event) error {
		calls.Add(1)
		return errors.New("always-fail")
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "jobs", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.Stats().DeadLettered == 1 }) {
		t.Fatal("未进入死信")
	}
	time.Sleep(80 * time.Millisecond) // 再等若干调度周期确认不会继续重试
	if got := calls.Load(); got != 2 {
		t.Fatalf("有界重试判定：首投+1次重试=2 次，实际 %d（存在无限重试）", got)
	}
	t.Logf("关键判定：MaxRetries=1 时总投递次数=%d，重试有界且已入死信", calls.Load())
}
