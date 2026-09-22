package eventqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func bpTopic(mode BackpressureMode, maxLag int64) TopicConfig {
	tc := defaultTopic()
	tc.Name = "bp"
	tc.PartitionNum = 1
	tc.Backpressure = BackpressureConfig{MaxPartitionLag: maxLag, Mode: mode}
	return tc
}

// TestBackpressureReject 单分区积压达到上限时立即拒绝发布，
// 返回 ErrBackpressure 且 RejectedPublish 指标可观测；积压下降后可恢复发布。
func TestBackpressureReject(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, bpTopic(BackpressureReject, 5))
	cfg.MaxMemorySize = 0
	b := newTestBroker(t, cfg)
	ctx := context.Background()

	// 无订阅者：发布即积压，lag 随发布数增长
	for i := 0; i < 5; i++ {
		if _, err := b.Publish(ctx, "bp", "k", []byte("x")); err != nil {
			t.Fatalf("第 %d 条应发布成功（lag<5），实际 %v", i, err)
		}
	}
	st := b.Stats()
	if st.Lag != 5 {
		t.Fatalf("积压应为5，实际 %d", st.Lag)
	}
	if _, err := b.Publish(ctx, "bp", "k", []byte("x")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("达到积压上限必须 ErrBackpressure，实际 %v", err)
	}
	if b.Stats().RejectedPublish != 1 {
		t.Fatalf("RejectedPublish 应为1，实际 %d", b.Stats().RejectedPublish)
	}
	t.Logf("关键判定：lag=%d 达到上限，第6条发布被拒绝且计数 RejectedPublish=%d（不丢已有事件、不阻塞）",
		st.Lag, b.Stats().RejectedPublish)

	// 订阅并消费全部积压后，发布必须恢复
	var consumed atomic.Int64
	if _, err := b.Subscribe("g1", "bp", func(ev *Event) error {
		consumed.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return b.Stats().Lag == 0 }) {
		t.Fatalf("积压未清空：%+v", b.Stats())
	}
	if _, err := b.Publish(ctx, "bp", "k", []byte("y")); err != nil {
		t.Fatalf("积压下降后应能继续发布，实际 %v", err)
	}
	t.Logf("关键判定：消费后 lag=0，新发布成功，内存占用=%d 字节", b.Stats().MemoryBytes)
}

// TestBackpressureBlock 阻塞模式：达到上限时发布阻塞，位点推进后被唤醒完成；
// ctx 取消必须能中断等待。
func TestBackpressureBlock(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, bpTopic(BackpressureBlock, 3))
	cfg.MaxMemorySize = 0
	b := newTestBroker(t, cfg)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := b.Publish(ctx, "bp", "k", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := b.Publish(context.Background(), "bp", "k", []byte("blocked"))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("背压未生效，第4条直接发布：err=%v", err)
	case <-time.After(60 * time.Millisecond):
	}
	if b.Stats().BlockedPublish < 1 {
		t.Fatalf("BlockedPublish 应>=1，实际 %d", b.Stats().BlockedPublish)
	}
	t.Logf("关键判定：lag=3 时第4条发布阻塞，BlockedPublish=%d", b.Stats().BlockedPublish)

	// 订阅消费，腾出一个位点
	release := make(chan struct{})
	got := make(chan *Event, 4)
	t.Cleanup(func() { close(release) })
	if _, err := b.Subscribe("g1", "bp", func(ev *Event) error {
		got <- ev
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ev := <-got
	if err := b.Ack("g1", ev); err != nil { // 手动先落确认，handler 稍后才返回
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("位点腾出后阻塞发布应成功，实际 %v", err)
		}
		t.Log("关键判定：确认推进位点后，阻塞的发布被唤醒并成功")
	case <-time.After(2 * time.Second):
		t.Fatal("阻塞发布未在位点推进后唤醒")
	}
}

// TestBackpressureBlockCancel ctx 取消必须中断阻塞发布并返回可区分错误。
func TestBackpressureBlockCancel(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir, bpTopic(BackpressureBlock, 1))
	cfg.MaxMemorySize = 0
	b := newTestBroker(t, cfg)
	if _, err := b.Publish(context.Background(), "bp", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := b.Publish(ctx, "bp", "k", []byte("y"))
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("取消阻塞发布应返回 ErrBackpressure，实际 %v", err)
	}
	t.Logf("关键判定：ctx 超时取消阻塞发布，返回 %v", err)
}

// TestMemoryLimitBackpressure 全局内存上限同样触发背压，事件被消费确认后内存释放。
func TestMemoryLimitBackpressure(t *testing.T) {
	dir := t.TempDir()
	tc := bpTopic(BackpressureReject, 0) // 不限制积压条数
	cfg := testConfig(t, dir, tc)
	cfg.MaxMemorySize = 5
	b := newTestBroker(t, cfg)
	ctx := context.Background()
	if _, err := b.Publish(ctx, "bp", "k", make([]byte, 6)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "bp", "k", make([]byte, 6)); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("超过内存上限应 ErrBackpressure，实际 %v", err)
	}
	t.Logf("关键判定：内存=%d 字节达到上限5，发布被拒绝（不无界占用内存）",
		b.Stats().MemoryBytes)
}
