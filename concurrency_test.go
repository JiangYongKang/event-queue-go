package queue

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestConcurrentOutOfOrderAck 8 个消费者、窗口=4 并行处理同一分区：
// 处理耗时随机 => 确认乱序到达；断言位点从不回退、最终精确到达 n，且无重复副作用。
func TestConcurrentOutOfOrderAck(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxInFlightPerPartition = 4
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("c", 1); err != nil {
		t.Fatal(err)
	}

	const n = 200
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var effectMu sync.Mutex
	effects := make([]int, n)
	var maxCommitted uint64
	var advMu sync.Mutex

	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(42))
	consume := func(c *Consumer) {
		defer wg.Done()
		for {
			ev, err := c.Poll(ctx)
			if err != nil {
				return
			}
			time.Sleep(time.Duration(rng.Intn(2)) * time.Millisecond)
			effectMu.Lock()
			effects[ev.Offset]++
			effectMu.Unlock()
			if err := c.Ack(ev); err != nil {
				t.Errorf("Ack offset=%d: %v", ev.Offset, err)
				return
			}
			// 直接观测分区位点，断言单调不减。
			// 注意：读位点与更新 maxCommitted 必须在同一临界区内整体完成，
			// 否则多 goroutine 的“读-比较”交错会产生假回退。
			advMu.Lock()
			co := b.topics["c"].partitions[0].committedOffset("g")
			if co < maxCommitted {
				t.Errorf("offset regression: %d -> %d", maxCommitted, co)
			}
			if co > maxCommitted {
				maxCommitted = co
			}
			advMu.Unlock()
		}
	}
	for i := 0; i < 8; i++ {
		c, err := b.Subscribe("c", "g", "c"+itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go consume(c)
	}

	for i := 0; i < n; i++ {
		if _, _, err := b.Publish(ctx, "c", "k", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool { return b.Stats().Acked == uint64(n) }, "all acked")
	cancel()
	wg.Wait()

	effectMu.Lock()
	defer effectMu.Unlock()
	for off, cnt := range effects {
		if cnt != 1 {
			t.Fatalf("offset %d side effect count=%d, want exactly 1", off, cnt)
		}
	}
	if co := b.topics["c"].partitions[0].committedOffset("g"); co != n {
		t.Fatalf("committed=%d, want %d", co, n)
	}
	t.Logf("判定依据: 每个 offset 副作用恰好 1 次（%d 个事件）；并发观测位点单调递增，最终 committed=%d；Stats=%+v",
		n, b.topics["c"].partitions[0].committedOffset("g"), b.Stats())
}

// TestStaleAndDuplicateAckUnderRetry 事件重试后，持有旧副本的确认必须被拒绝（位点回退），
// 旧副本重复确认也必须拒绝；只有当前版本可以确认。
func TestStaleAndDuplicateAckUnderRetry(t *testing.T) {
	b, err := NewBroker(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("s", 1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c1, err := b.Subscribe("s", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}

	ready := make(chan Event, 4)
	go func() {
		ev, _ := c1.Poll(ctx)
		ready <- ev
	}()
	if _, _, err := b.Publish(ctx, "s", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	first := <-ready

	// 用第一版副本 Nack 触发重试。
	if err := c1.Nack(first, errBoom); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	go func() {
		ev, _ := c1.Poll(ctx)
		ready <- ev
	}()
	second := <-ready
	if second.Attempts != first.Attempts+1 {
		t.Fatalf("redelivery attempts=%d, want %d", second.Attempts, first.Attempts+1)
	}

	// 旧版本 Ack：位点回退，必须拒绝。
	if err := c1.Ack(first); !errorIs(err, ErrOffsetRegression) {
		t.Fatalf("stale ack want ErrOffsetRegression, got %v", err)
	}
	// 当前版本确认成功。
	if err := c1.Ack(second); err != nil {
		t.Fatalf("Ack second: %v", err)
	}
	// 旧版本重复确认。
	if err := c1.Ack(first); !errorIs(err, ErrDuplicateAck) {
		t.Fatalf("stale duplicate want ErrDuplicateAck, got %v", err)
	}
	// 当前版本重复确认。
	if err := c1.Ack(second); !errorIs(err, ErrDuplicateAck) {
		t.Fatalf("duplicate want ErrDuplicateAck, got %v", err)
	}
	t.Logf("判定依据: 重试后 attempt %d->%d；旧副本 Ack=>ErrOffsetRegression；确认后新旧副本重复 Ack=>ErrDuplicateAck；committed=%d",
		first.Attempts, second.Attempts, b.topics["s"].partitions[0].committedOffset("g"))
}

var errBoom = errors.New("boom")
