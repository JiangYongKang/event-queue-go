package eventqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestMultipleGroupsIndependent 多个消费者组各自维护独立位点：
// 同一条事件每个组恰好消费一次，互不阻塞、位点互不影响。
func TestMultipleGroupsIndependent(t *testing.T) {
	dir := t.TempDir()
	tc := defaultTopic()
	tc.Name = "mg"
	tc.PartitionNum = 2
	b := newTestBroker(t, testConfig(t, dir, tc))

	var mu sync.Mutex
	seen := map[string]map[string]int{} // group -> eventKey -> count
	record := func(group string, ev *Event) error {
		mu.Lock()
		defer mu.Unlock()
		if seen[group] == nil {
			seen[group] = map[string]int{}
		}
		seen[group][evKey(ev)]++
		return nil
	}
	hGA := func(ev *Event) error { return record("ga", ev) }
	hGB := func(ev *Event) error { return record("gb", ev) }
	if _, err := b.Subscribe("ga", "mg", hGA); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("gb", "mg", hGB); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const n = 10
	for i := 0; i < n; i++ {
		if _, err := b.Publish(ctx, "mg", fmt.Sprintf("key%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(2*time.Second, func() bool {
		st := b.Stats()
		return st.Acked == int64(2*n) && st.Lag == 0
	}) {
		t.Fatalf("两组未各自消费完：%+v", b.Stats())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen["ga"]) != n || len(seen["gb"]) != n {
		t.Fatalf("每组应各见 %d 个不同事件，实际 ga=%d gb=%d", n, len(seen["ga"]), len(seen["gb"]))
	}
	for g, m := range seen {
		for k, c := range m {
			if c != 1 {
				t.Fatalf("组 %s 事件 %s 被处理 %d 次（期望1次，无可见重复副作用）", g, k, c)
			}
		}
	}
	t.Logf("关键判定：两组各自独立消费 %d 个事件，每组每事件恰好1次，acked=%d", n, b.Stats().Acked)
}

// TestSameGroupNoConcurrentDuplicate 同一组同一分区严格串行：
// 任意时刻同一 offset 只有一个在途投递，业务侧不会观察到同一事件并发执行。
func TestSameGroupNoConcurrentDuplicate(t *testing.T) {
	dir := t.TempDir()
	tc := defaultTopic()
	tc.Name = "sg"
	tc.PartitionNum = 1
	b := newTestBroker(t, testConfig(t, dir, tc))

	var mu sync.Mutex
	active := map[int64]int{}
	maxActive := 0
	var order []int64
	h := func(ev *Event) error {
		mu.Lock()
		active[ev.Offset]++
		if active[ev.Offset] > 1 {
			mu.Unlock()
			t.Errorf("同一 offset %d 出现并发重复投递", ev.Offset)
			return nil
		}
		cur := len(active)
		if cur > maxActive {
			maxActive = cur
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		order = append(order, ev.Offset)
		delete(active, ev.Offset)
		mu.Unlock()
		return nil
	}
	if _, err := b.Subscribe("g1", "sg", h); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := b.Publish(context.Background(), "sg", "k", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(2*time.Second, func() bool { return b.Stats().Lag == 0 }) {
		t.Fatalf("未消费完：%+v", b.Stats())
	}
	if maxActive != 1 {
		t.Fatalf("同组同分区在途峰值应为1（严格串行），实际 %d", maxActive)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 20 {
		t.Fatalf("应严格顺序处理20个事件，实际 %d", len(order))
	}
	for i, off := range order {
		if off != int64(i) {
			t.Fatalf("顺序错乱：%v", order)
		}
	}
	t.Logf("关键判定：同组同分区在途峰值=%d，严格顺序处理20事件，无并发重复投递", maxActive)
}
