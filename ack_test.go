package eventqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func manualTopic() TopicConfig {
	tc := defaultTopic()
	tc.Name = "manual"
	tc.PartitionNum = 1
	return tc
}

// TestDuplicateAndUnknownAckRejected 重复确认与对未知事件的确认必须被明确拒绝。
func TestDuplicateAndUnknownAckRejected(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, manualTopic()))

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) }) // 先于 broker 的 Cleanup 执行（LIFO），解除在途 handler
	received := make(chan *Event, 4)
	c, err := b.Subscribe("g1", "manual", func(ev *Event) error {
		received <- ev
		<-stop // 挂住以便手动确认；测试结束时统一放行，避免 goroutine 泄漏
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	e0, err := b.Publish(ctx, "manual", "k", []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	ev0 := <-received

	// 正常确认
	if err := c.Ack(ev0); err != nil {
		t.Fatalf("首次确认应成功：%v", err)
	}
	t.Logf("首次确认成功：topic=%s offset=%d", ev0.Topic, ev0.Offset)

	// 重复确认 -> ErrDuplicateAck
	err = c.Ack(ev0)
	if !errors.Is(err, ErrDuplicateAck) {
		t.Fatalf("重复确认必须返回 ErrDuplicateAck，实际 %v", err)
	}
	t.Logf("重复确认被拒绝：%v（位点不回退）", err)

	// 对从未投递的 offset 确认 -> ErrUnknownEvent
	ghost := *e0
	ghost.Offset = 99
	err = c.Ack(&ghost)
	if !errors.Is(err, ErrUnknownEvent) {
		t.Fatalf("未知 offset 确认必须返回 ErrUnknownEvent，实际 %v", err)
	}
	t.Logf("未知事件确认被拒绝：%v", err)

	// 投出 offset1
	e1, err := b.Publish(ctx, "manual", "k", []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	ev1 := <-received
	// 篡改 EventID -> ErrUnknownEvent
	fake := *e1
	fake.ID = "deadbeef"
	if err := c.Ack(&fake); !errors.Is(err, ErrUnknownEvent) {
		t.Fatalf("ID 不匹配的确认必须返回 ErrUnknownEvent，实际 %v", err)
	}
	if err := c.Ack(ev1); err != nil {
		t.Fatalf("正常确认 ev1 失败：%v", err)
	}
	t.Logf("ID 篡改确认被拒绝；真实 ev1 确认成功")
}

// TestConcurrentAckCommitMonotonic 并发安全核心测试：
// 直接向提交路径注入一批“在途”事件，然后逆序高并发确认，
// 监控 committed 在整个过程中单调递增、最终等于发布数。
func TestConcurrentAckCommitMonotonic(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, manualTopic()))
	ctx := context.Background()

	const n = 50
	evs := make([]*Event, n)
	for i := 0; i < n; i++ {
		var err error
		evs[i], err = b.Publish(ctx, "manual", "k", []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// 白盒注入：不经过订阅（调度器不会介入），直接构造组状态与在途表。
	pk := pKey{topic: "manual", partition: 0}
	p := b.partitions[pk]
	g := p.getOrCreateGroup("g1", 0)
	for i := 0; i < n; i++ {
		g.inflight[int64(i)] = &inflightEvent{ev: evs[i], attempts: 1}
	}

	// 监控 committed：全程不得回退
	stopMon := make(chan struct{})
	var violated atomic.Bool
	go func() {
		var last int64 = -1
		for {
			select {
			case <-stopMon:
				return
			default:
			}
			b.mu.RLock()
			cur := g.committed
			b.mu.RUnlock()
			if cur < last {
				violated.Store(true)
				return
			}
			last = cur
		}
	}()

	// 逆序并发确认：水位只在连续前缀补齐时跳跃式前进
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := n - 1; i >= 0; i-- {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			time.Sleep(time.Duration(idx%7) * time.Millisecond)
			errs[idx] = b.finishAck(pk, "g1", evs[idx], 1, nil)
		}(i)
	}
	wg.Wait()
	close(stopMon)
	for i, e := range errs {
		if e != nil {
			t.Fatalf("offset %d 确认失败：%v", i, e)
		}
	}
	if violated.Load() {
		t.Fatal("监控发现 committed 出现回退")
	}
	if g.committed != n {
		t.Fatalf("committed 应为 %d，实际 %d", n, g.committed)
	}
	t.Logf("关键位点：%d 个逆序并发确认后 committed=%d，过程中单调递增无回退", n, g.committed)
}

// TestConcurrentDuplicateAck 对同一个在途事件并发确认：
// 恰好 1 个成功，其余全部 ErrDuplicateAck，位点只推进一次。
func TestConcurrentDuplicateAck(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, manualTopic()))
	ev, err := b.Publish(context.Background(), "manual", "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	pk := pKey{topic: "manual", partition: 0}
	g := b.partitions[pk].getOrCreateGroup("g1", 0)
	g.inflight[0] = &inflightEvent{ev: ev, attempts: 1}

	const m = 100
	var wg sync.WaitGroup
	var ok, dup atomic.Int64
	start := make(chan struct{})
	for i := 0; i < m; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := b.finishAck(pk, "g1", ev, 1, nil)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrDuplicateAck):
				dup.Add(1)
			default:
				t.Errorf("非预期错误：%v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok.Load() != 1 || dup.Load() != m-1 {
		t.Fatalf("并发确认判定：成功=%d（期望1），重复拒绝=%d（期望%d）", ok.Load(), dup.Load(), m-1)
	}
	if g.committed != 1 {
		t.Fatalf("committed 只应推进到1，实际 %d", g.committed)
	}
	t.Logf("关键判定：%d 个并发确认中 1 成功、%d 个重复拒绝，committed=%d 无错位",
		m, m-1, g.committed)
}

// TestUnknownTopicAndBadConfig 未知主题/非法配置返回可区分错误。
func TestUnknownTopicAndBadConfig(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, manualTopic()))

	if _, err := b.Publish(context.Background(), "nope", "k", nil); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("未知主题应 ErrUnknownTopic，实际 %v", err)
	}
	if _, err := b.Subscribe("g", "nope", func(*Event) error { return nil }); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("订阅未知主题应 ErrUnknownTopic，实际 %v", err)
	}
	t.Log("未知主题错误可区分")

	bad := Config{DataDir: "", Topics: []TopicConfig{{Name: "x", PartitionNum: 1}}}
	if _, err := Open(bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("空 DataDir 应 ErrInvalidConfig，实际 %v", err)
	}
	bad2 := Config{DataDir: t.TempDir(), Topics: []TopicConfig{{Name: "x", PartitionNum: 0}}}
	if _, err := Open(bad2); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("PartitionNum=0 应 ErrInvalidConfig，实际 %v", err)
	}
	bad3 := testConfig(t, t.TempDir(), TopicConfig{
		Name: "x", PartitionNum: 1,
		Retry: RetryPolicy{MaxRetries: 2, Multiplier: 0.5},
	})
	if _, err := Open(bad3); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Multiplier<1 应 ErrInvalidConfig，实际 %v", err)
	}
	bad4 := testConfig(t, t.TempDir(), manualTopic())
	bad4.MaxMemorySize = -1
	if _, err := Open(bad4); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("负 MaxMemorySize 应 ErrInvalidConfig，实际 %v", err)
	}
	t.Log("非法配置均返回 ErrInvalidConfig，错误信息可区分")
}

// TestErrorMessagesDoNotLeakInternals 对外错误不携带底层路径/系统错误细节。
func TestErrorMessagesDoNotLeakInternals(t *testing.T) {
	dir := t.TempDir()
	b := newTestBroker(t, testConfig(t, dir, manualTopic()))
	err := b.Ack("g1", &Event{Topic: "manual", Partition: 0, Offset: 12345, ID: "x"})
	if err == nil {
		t.Fatal("期望错误")
	}
	if strings.Contains(err.Error(), dir) {
		t.Fatalf("对外错误泄漏内部路径：%v", err)
	}
	t.Logf("对外错误（无内部细节）：%v", err)
}
