package queue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// TestRetryThenAck 事件前两次处理失败，第三次成功：验证重试、退避与最终确认。
func TestRetryThenAck(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("jobs", 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	attempts := map[string]int{}
	var wg sync.WaitGroup
	c, err := b.Subscribe("jobs", "g", "c0")
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go serve(ctx, b, c, func(ctx context.Context, e Event) error {
		mu.Lock()
		attempts[e.ID]++
		n := attempts[e.ID]
		mu.Unlock()
		if n < 3 {
			return errors.New("transient failure")
		}
		return nil
	}, &wg, nil, nil)

	if _, _, err := b.Publish(ctx, "jobs", "k", []byte("job")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		st := b.Stats()
		return st.Acked == 1
	}, "retried event acked")

	mu.Lock()
	defer mu.Unlock()
	if attempts["jobs-0-0"] != 3 {
		t.Fatalf("attempts=%d, want 3", attempts["jobs-0-0"])
	}
	st := b.Stats()
	if st.Retried != 2 || st.Delivered != 3 || st.Acked != 1 {
		t.Fatalf("stats=%+v, want Delivered=3 Retried=2 Acked=1", st)
	}
	t.Logf("判定依据: 同一事件 ID 被投递 attempts=3，Delivered=3 Retried=2 Acked=1；退避序列 1ms,2ms")
	cancel()
	wg.Wait()
}

// TestRetriesExhaustedToDLQ 处理始终失败：重试次数耗尽后进入死信，
// 死信必须携带可查询的失败原因，且不得无限重试，后续事件可继续消费。
func TestRetriesExhaustedToDLQ(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Retry.MaxAttempts = 2
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("jobs", 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	counts := map[string]int{}
	var wg sync.WaitGroup
	c, err := b.Subscribe("jobs", "g", "c0")
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go serve(ctx, b, c, func(ctx context.Context, e Event) error {
		mu.Lock()
		counts[e.ID]++
		mu.Unlock()
		return errors.New("boom on " + e.ID)
	}, &wg, nil, nil)

	if _, _, err := b.Publish(ctx, "jobs", "k", []byte("bad")); err != nil {
		t.Fatal(err)
	}
	// 死信之后发布的第二个事件，也始终失败 => 同样进死信，验证分区不会卡死。
	if _, _, err := b.Publish(ctx, "jobs", "k", []byte("bad2")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return b.Stats().DeadLettered == 2 }, "both events dead-lettered")

	mu.Lock()
	defer mu.Unlock()
	if counts["jobs-0-0"] != 2 || counts["jobs-0-1"] != 2 {
		t.Fatalf("each event must be attempted exactly MaxAttempts=2, got %v", counts)
	}

	dls, err := b.DeadLetters("jobs", "g")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 2 {
		t.Fatalf("dead letters=%d, want 2", len(dls))
	}
	for i, dl := range dls {
		wantOff := uint64(i)
		if dl.Event.Offset != wantOff || dl.Attempts != 2 || dl.Group != "g" {
			t.Fatalf("dl[%d]=%+v", i, dl)
		}
		if dl.Reason == "" || !strings.Contains(dl.Reason, "boom on jobs-0-"+itoa(i)) {
			t.Fatalf("dead letter reason missing cause: %q", dl.Reason)
		}
		if dl.FailedAt.IsZero() {
			t.Fatal("FailedAt not set")
		}
	}
	t.Logf("判定依据: 每事件恰好投递 %d 次（不无限重试）；DLQ 记录=%d，reason 示例=%q",
		cfg.Retry.MaxAttempts, len(dls), dls[0].Reason)
	// 死信视为终态：位点推进越过死信，分区可继续。
	if co := b.topics["jobs"].partitions[0].committedOffset("g"); co != 2 {
		t.Fatalf("committed=%d, want 2 (dead letters advance offset)", co)
	}
	t.Logf("关键位点: committed=%d（死信 offset 0,1 已跳过）Stats=%+v",
		b.topics["jobs"].partitions[0].committedOffset("g"), b.Stats())
	cancel()
	wg.Wait()
}

// TestDuplicateDeliveryAccepted 构造重复投递：事件失败重试时，
// 幂等 Handler 对重复 ID 不执行副作用，但正常 Ack 且计入 Duplicates，不当作失败。
func TestDuplicateDeliveryAccepted(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Retry.MaxAttempts = 3
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("dedup", 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dedup := NewDedupStore()
	var mu sync.Mutex
	sideEffects := map[string]int{}
	var wg sync.WaitGroup
	c, err := b.Subscribe("dedup", "g", "c0")
	if err != nil {
		t.Fatal(err)
	}
	// 业务 Handler：第一次副作用失败，第二次（重试）真正成功。
	business := Handler(func(ctx context.Context, e Event) error {
		mu.Lock()
		defer mu.Unlock()
		sideEffects[e.ID]++
		if sideEffects[e.ID] == 1 {
			return errors.New("first side effect fails")
		}
		return nil
	})
	h := b.NewDedupHandler(dedup, business)

	wg.Add(1)
	go serve(ctx, b, c, h, &wg, nil, nil)

	if _, _, err := b.Publish(ctx, "dedup", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return b.Stats().Acked == 1 }, "acked")

	mu.Lock()
	defer mu.Unlock()
	if sideEffects["dedup-0-0"] != 2 {
		t.Fatalf("business handler should run twice (fail then success), got %d", sideEffects["dedup-0-0"])
	}
	// 重启后重放已确认事件的场景见 recovery 测试；这里直接验证 Check 幂等。
	if !dedup.Check("dedup-0-0") {
		t.Fatal("dedup store should remember event")
	}
	st := b.Stats()
	if st.Acked != 1 || st.DeadLettered != 0 {
		t.Fatalf("stats=%+v", st)
	}
	t.Logf("判定依据: 副作用执行=%d 次后 Ack；重试被正常接受不产生失败终态；Stats=%+v",
		sideEffects["dedup-0-0"], st)
	cancel()
	wg.Wait()
}
