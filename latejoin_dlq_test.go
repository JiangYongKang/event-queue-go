package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLateSubscriberHistoryRetryExhaustedToDLQ 复现并验证：
// 历史事件已被早到消费组全部确认、内存窗口完全腾空后，晚到消费组补消费这段
// “只在磁盘上”的历史；若它一直处理失败、重试次数耗尽，语义必须与事件还在
// 内存时完全一致——进入死信（携因、可按组查询）、位点连续越过、后续实时
// 事件照常消费，进程不得崩溃；同分区其他消费组（early / peer）的位点、
// 去重与计数不受影响，也不会错分组。
func TestLateSubscriberHistoryRetryExhaustedToDLQ(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Retry.MaxAttempts = 2 // 缩小次数，退避沿用 1ms 起，稳定快速复现
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("hdq", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := b.topics["hdq"].partitions[0]

	const n = 6
	// 1) 早到组在线消费并确认全部历史（后台持续服务，后面还要吃实时事件）。
	early, err := b.Subscribe("hdq", "early", "c0")
	if err != nil {
		t.Fatal(err)
	}
	var earlyWG sync.WaitGroup
	earlyWG.Add(1)
	go serve(ctx, b, early, Handler(func(ctx context.Context, e Event) error { return nil }), &earlyWG, nil, nil)
	for i := 0; i < n; i++ {
		if _, _, err := b.Publish(ctx, "hdq", "k", []byte(fmt.Sprintf("h%02d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, func() bool { return p.committedOffset("early") == n }, "early group caught up")
	base, win := p.memWindow()
	if base != n || win != 0 {
		t.Fatalf("memWindow base=%d len=%d, want base=%d len=0", base, win, n)
	}
	t.Logf("历史已腾空: early.committed=%d，内存窗口 base=%d len=%d（历史仅在磁盘）",
		p.committedOffset("early"), base, win)

	// 2) 晚到组上线补历史：处理器先被 gate 挡住，取走首条但不返回结果，
	//    使“补历史刚开始”的积压/容量观测点确定不受调度时序影响；
	//    同分区再加一个全程成功的 peer 组，验证补历史失败不影响他组。
	gate := make(chan struct{})
	late, err := b.Subscribe("hdq", "late", "c1")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := b.Subscribe("hdq", "peer", "c2")
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	attempts := map[string]int{}
	var lateWG sync.WaitGroup
	lateWG.Add(1)
	go serve(ctx, b, late, Handler(func(ctx context.Context, e Event) error {
		<-gate // 观测点完成前不返回，保证历史尚未被解决
		mu.Lock()
		attempts[e.ID]++
		mu.Unlock()
		return errors.New("late boom " + e.ID)
	}), &lateWG, nil, nil)

	dedup := NewDedupStore()
	peerSideEffects := 0
	var peerWG sync.WaitGroup
	peerWG.Add(1)
	peerHandler := b.NewDedupHandler(dedup, Handler(func(ctx context.Context, e Event) error {
		mu.Lock()
		peerSideEffects++
		mu.Unlock()
		return nil
	}))
	go serve(ctx, b, peer, peerHandler, &peerWG, nil, nil)

	// 观测点：内存窗口为空，但积压须按确认水位（late=0）如实核算，
	// 已逐出部分的字节由 evictedBytes 补足——容量判定不依赖内存命中。
	waitFor(t, func() bool { return b.Backlog()[0].InFlight >= 1 }, "late group received first history event")
	if m := b.Backlog()[0]; m.Pending != n || m.PayloadBytes != n*3 {
		t.Fatalf("catch-up backlog=%+v, want Pending=%d Bytes=%d", m, n, n*3)
	}
	t.Logf("补历史开始: 容量上限条数=%d 字节=%d（0 表示不限）；Backlog=%+v（历史不在内存仍计入）",
		cfg.MaxBacklogPerPartition, cfg.MaxMemoryBytes, b.Backlog()[0])
	close(gate) // 放行：晚到组开始一直失败

	// 3) 整段磁盘历史重试耗尽。能跑到这里本身就证明派发协程没有把进程带崩。
	waitFor(t, func() bool { return b.Stats().DeadLettered == n }, "all disk history dead-lettered")
	waitFor(t, func() bool { return p.committedOffset("late") == n }, "late committed advances past DLQ")
	waitFor(t, func() bool { return p.committedOffset("peer") == n }, "peer group caught up")

	mu.Lock()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("hdq-0-%d", i)
		if attempts[id] != cfg.Retry.MaxAttempts {
			t.Fatalf("%s attempts=%d, want %d（不得无限重试，也不得少投）",
				id, attempts[id], cfg.Retry.MaxAttempts)
		}
	}
	if peerSideEffects != n {
		t.Fatalf("peer side effects=%d, want %d（补历史失败不得错投/影响他组）", peerSideEffects, n)
	}
	mu.Unlock()

	dls, err := b.DeadLetters("hdq", "late")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != n {
		t.Fatalf("late dead letters=%d, want %d", len(dls), n)
	}
	for i, dl := range dls {
		if dl.Group != "late" || dl.Event.Offset != uint64(i) || dl.Attempts != cfg.Retry.MaxAttempts {
			t.Fatalf("dl[%d]=%+v", i, dl)
		}
		if want := fmt.Sprintf("h%02d", i); string(dl.Event.Payload) != want {
			t.Fatalf("dl[%d] payload=%q, want %q（死信须携带磁盘上的事件本体）", i, dl.Event.Payload, want)
		}
		if !strings.Contains(dl.Reason, "late boom hdq-0-"+itoa(i)) {
			t.Fatalf("dl[%d] reason=%q 缺少失败原因", i, dl.Reason)
		}
	}
	if peerDLs, _ := b.DeadLetters("hdq", "peer"); len(peerDLs) != 0 {
		t.Fatalf("peer dead letters=%d, want 0（不得错分到别的组）", len(peerDLs))
	}
	t.Logf("历史耗尽: late.committed=%d 已越过全部死信；peer.committed=%d；DLQ[late]=%d；Backlog=%+v",
		p.committedOffset("late"), p.committedOffset("peer"), len(dls), b.Backlog()[0])

	// 4) 补历史结束后，实时新事件对三个组都继续推进：late 失败进死信、
	//    early/peer 成功确认，分区既不卡死也不吞事件。
	if _, _, err := b.Publish(ctx, "hdq", "k", []byte("live")); err != nil {
		t.Fatal(err)
	}
	liveID := "hdq-0-" + itoa(n)
	waitFor(t, func() bool { return b.Stats().DeadLettered == n+1 }, "live event also handled after history DLQ")
	waitFor(t, func() bool { return p.committedOffset("late") == n+1 }, "late committed after live")
	waitFor(t, func() bool { return p.committedOffset("peer") == n+1 }, "peer committed after live")
	waitFor(t, func() bool { return p.committedOffset("early") == n+1 }, "early committed after live")

	mu.Lock()
	if attempts[liveID] != cfg.Retry.MaxAttempts {
		t.Fatalf("live event attempts=%d, want %d", attempts[liveID], cfg.Retry.MaxAttempts)
	}
	if peerSideEffects != n+1 {
		t.Fatalf("peer side effects=%d after live, want %d（去重计数保持一致）", peerSideEffects, n+1)
	}
	mu.Unlock()

	if m := b.Backlog()[0]; m.Pending != 0 || m.PayloadBytes != 0 {
		t.Fatalf("final backlog=%+v, want zero", m)
	}
	st := b.Stats()
	if st.Duplicates != 0 {
		t.Fatalf("Duplicates=%d, want 0", st.Duplicates)
	}
	if st.DeadLettered != uint64(n+1) {
		t.Fatalf("DeadLettered=%d, want %d", st.DeadLettered, n+1)
	}
	if st.Acked != uint64(2*(n+1)) { // early 与 peer 各确认 n+1 条
		t.Fatalf("Acked=%d, want %d", st.Acked, 2*(n+1))
	}
	t.Logf("判定依据: 实时事件后 late/early/peer.committed=%d（死信越过、分区继续）；Backlog=%+v；Stats=%+v",
		p.committedOffset("late"), b.Backlog()[0], st)

	cancel()
	earlyWG.Wait()
	lateWG.Wait()
	peerWG.Wait()
}

// TestLateSubscriberHistoryMixedRetryAndDLQ 磁盘历史上的混合结果：
// 一条（任意中间位点）始终失败耗尽进死信；一条前两次失败、第三次成功，
// 验证重试未达上限时按退避正常重投并最终确认；其余直接成功。
// 位点必须连续越过死信推进到末尾，投递顺序、Attempts 版本与计数不变。
func TestLateSubscriberHistoryMixedRetryAndDLQ(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Retry.MaxAttempts = 3
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("hmix", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := b.topics["hmix"].partitions[0]

	const n = 5
	const dlOff = uint64(2) // 任意中间位点耗尽：覆盖“不是第一条也不是最后一条”
	const recoverOff = uint64(3)
	early, err := b.Subscribe("hmix", "early", "c0")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, _, err := b.Publish(ctx, "hmix", "k", []byte(fmt.Sprintf("m%02d", i))); err != nil {
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
	waitFor(t, func() bool { return p.committedOffset("early") == n }, "early caught up")
	if base, win := p.memWindow(); base != n || win != 0 {
		t.Fatalf("memWindow base=%d len=%d, want %d/0", base, win, n)
	}
	t.Logf("历史已腾空: base=%d len=0；死信位点=%d，退避后恢复位点=%d", n, dlOff, recoverOff)

	late, err := b.Subscribe("hmix", "late", "c1")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	attempts := map[uint64]int{}
	retryDelays := []time.Duration{}
	lastAttemptAt := map[uint64]time.Time{}
	var wg sync.WaitGroup
	wg.Add(1)
	go serve(ctx, b, late, Handler(func(ctx context.Context, e Event) error {
		mu.Lock()
		attempts[e.Offset]++
		nth := attempts[e.Offset]
		if prev, ok := lastAttemptAt[e.Offset]; ok && e.Offset == recoverOff {
			retryDelays = append(retryDelays, time.Since(prev))
		}
		lastAttemptAt[e.Offset] = time.Now()
		mu.Unlock()
		switch e.Offset {
		case dlOff:
			return errors.New("always bad")
		case recoverOff:
			if nth < 3 {
				return errors.New("temporary")
			}
		}
		return nil
	}), &wg, nil, nil)

	waitFor(t, func() bool { return p.committedOffset("late") == n }, "late catches up past mixed history")

	mu.Lock()
	for off := 0; off < n; off++ {
		want := 1
		if uint64(off) == dlOff {
			want = cfg.Retry.MaxAttempts
		}
		if uint64(off) == recoverOff {
			want = 3
		}
		if attempts[uint64(off)] != want {
			t.Fatalf("offset %d attempts=%d, want %d", off, attempts[uint64(off)], want)
		}
	}
	if len(retryDelays) != 2 {
		t.Fatalf("retry delay samples=%d, want 2", len(retryDelays))
	}
	for _, d := range retryDelays {
		// 退避序列 1ms、2ms；给足调度余量，只断言确实等待过（非无限紧循环）。
		if d < time.Millisecond/2 {
			t.Fatalf("retry delivered too soon: %v（退避未生效）", d)
		}
	}
	mu.Unlock()

	dls, err := b.DeadLetters("hmix", "late")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Event.Offset != dlOff || dls[0].Attempts != cfg.Retry.MaxAttempts {
		t.Fatalf("dead letters=%+v, want single offset %d", dls, dlOff)
	}
	if !strings.Contains(dls[0].Reason, "always bad") {
		t.Fatalf("dl reason=%q", dls[0].Reason)
	}
	st := b.Stats()
	if st.DeadLettered != 1 || st.Retried != 4 { // 死信事件重试 2 次 + 恢复事件重试 2 次
		t.Fatalf("stats=%+v, want DeadLettered=1 Retried=4", st)
	}
	// Acked 为 Broker 全局计数：early 确认 5 条 + late 成功确认 4 条。
	if st.Acked != uint64(n+(n-1)) {
		t.Fatalf("Acked=%d, want %d", st.Acked, n+(n-1))
	}
	t.Logf("判定依据: 位点=%d 连续越过死信位点 %d；各事件投递次数=%v；退避样本=%v；Stats=%+v",
		p.committedOffset("late"), dlOff, attempts, retryDelays, st)
	cancel()
	wg.Wait()
}
