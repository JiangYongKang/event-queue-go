package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestLateSubscriberHistoryRetryExhaustedToDLQ 晚到消费组补消费已逐出内存的历史：
// 早到组把历史全部确认、内存窗口腾空后，晚到组才订阅并从头补历史；
// 晚到组对历史中的第一条（offset 0）和任意一条（offset 3）始终处理失败到重试耗尽，
// offset 1 先失败一次再成功（验证补历史期间退避重投不变）。
// 要求：进程不崩；死信可按组查到记录与原因；位点连续越过死信推进到最新；
// 不无限重试、不卡死分区；在途版本/重复确认/位点回退等既有判定不变；
// 同分区其他组与实时新事件不受影响；积压与字节容量指标如实。
func TestLateSubscriberHistoryRetryExhaustedToDLQ(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Retry.MaxAttempts = 3
	cfg.MaxBacklogPerPartition = 64
	cfg.MaxMemoryBytes = 64 // 补历史期间实际积压远小于此，不得误判触限
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("dl", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := b.topics["dl"].partitions[0]

	const hist = 6
	const total = 10

	// 早到组先在场：消费并确认全部历史，随后继续消费实时新事件。
	var earlyMu sync.Mutex
	earlyErrs := []error{}
	var earlyWG sync.WaitGroup
	early, err := b.Subscribe("dl", "early", "c-early")
	if err != nil {
		t.Fatal(err)
	}
	earlyWG.Add(1)
	go serve(ctx, b, early, Handler(func(ctx context.Context, e Event) error {
		return nil
	}), &earlyWG, &earlyErrs, &earlyMu)

	for i := 0; i < hist; i++ {
		if _, _, err := b.Publish(ctx, "dl", "k", []byte(fmt.Sprintf("h%02d", i))); err != nil {
			t.Fatalf("publish history %d: %v", i, err)
		}
	}
	waitFor(t, func() bool { return p.committedOffset("early") == hist }, "early group acks history")
	waitFor(t, func() bool {
		base, n := p.memWindow()
		return base == hist && n == 0
	}, "memory window fully evicted")
	if m := b.Backlog()[0]; m.Pending != 0 || m.PayloadBytes != 0 {
		t.Fatalf("backlog before late join=%+v, want empty", m)
	}
	t.Logf("[阶段1] 早到组确认历史 %d 条：内存窗口 base=%d len=%d（历史仅在磁盘）；"+
		"Backlog=%+v；容量判定 Pending/上限=%d/%d",
		hist, mustBase(p), mustLen(p), b.Backlog()[0], b.Backlog()[0].Pending, cfg.MaxBacklogPerPartition)

	// 晚到组在内存一条历史都不剩时订阅：先观察积压如实回到 hist。
	var lateMu sync.Mutex
	lateErrs := []error{}
	var lateWG sync.WaitGroup
	late, err := b.Subscribe("dl", "late", "c-late")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		m := b.Backlog()[0]
		return m.Pending == hist && m.PayloadBytes == hist*3
	}, "backlog reflects late group replaying from disk")
	t.Logf("[阶段2] 晚到组订阅：历史全部需从磁盘回读；Backlog=%+v（%d 条、%d 字节，"+
		"容量判定 %d/%d 条、%d/%d 字节，未触限）",
		b.Backlog()[0], hist, hist*3,
		b.Backlog()[0].Pending, cfg.MaxBacklogPerPartition,
		b.Backlog()[0].PayloadBytes, cfg.MaxMemoryBytes)

	// 晚到组处理逻辑：0 与 3 始终失败（耗尽 => 死信）；1 首次失败、第二次成功；其余一次成功。
	alwaysFail := map[uint64]bool{0: true, 3: true}
	attemptCnt := map[uint64]int{}
	var deliveryOrder []uint64
	lateHandler := Handler(func(ctx context.Context, e Event) error {
		lateMu.Lock()
		defer lateMu.Unlock()
		attemptCnt[e.Offset]++
		deliveryOrder = append(deliveryOrder, e.Offset)
		if alwaysFail[e.Offset] {
			return fmt.Errorf("history boom at offset %d", e.Offset)
		}
		if e.Offset == 1 && e.Attempts == 1 {
			return errors.New("transient failure at offset 1")
		}
		return nil
	})
	lateWG.Add(1)
	go serve(ctx, b, late, lateHandler, &lateWG, &lateErrs, &lateMu)

	// 补历史进行中发布实时新事件：早到组照常消费，晚到组补完历史后也会接到。
	for i := hist; i < total; i++ {
		if _, _, err := b.Publish(ctx, "dl", "k", []byte(fmt.Sprintf("n%02d", i))); err != nil {
			t.Fatalf("publish live %d: %v", i, err)
		}
	}

	// 第一条历史死信落定：位点必须已越过 offset 0 继续推进，分区没有被卡死。
	waitFor(t, func() bool { return b.Stats().DeadLettered >= 1 }, "first history event dead-lettered")
	waitFor(t, func() bool {
		m := b.Backlog()[0]
		return p.committedOffset("late") == 1 && m.Pending == total-1 && m.PayloadBytes == (total-1)*3
	}, "late group advances past first dead letter")
	t.Logf("[阶段3] offset 0 重试耗尽进入死信：late.committed=%d（已越过死信继续推进）；"+
		"Backlog=%+v（容量判定 %d/%d 条、%d/%d 字节）；early.committed=%d",
		p.committedOffset("late"), b.Backlog()[0],
		b.Backlog()[0].Pending, cfg.MaxBacklogPerPartition,
		b.Backlog()[0].PayloadBytes, cfg.MaxMemoryBytes, p.committedOffset("early"))

	// 全部终态：晚到组追平实时位点。
	waitFor(t, func() bool { return b.Stats().DeadLettered == 2 }, "both history dead letters recorded")
	waitFor(t, func() bool { return p.committedOffset("late") == total }, "late group catches up")
	waitFor(t, func() bool {
		m := b.Backlog()[0]
		return m.Pending == 0 && m.PayloadBytes == 0
	}, "backlog drained")

	// 死信可查：属于晚到组、携带失败原因与尝试次数；不会错分到早到组。
	dls, err := b.DeadLetters("dl", "late")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 2 {
		t.Fatalf("late dead letters=%d, want 2 (offsets 0 and 3)", len(dls))
	}
	for i, off := range []uint64{0, 3} {
		dl := dls[i]
		if dl.Event.Offset != off || dl.Group != "late" || dl.Attempts != cfg.Retry.MaxAttempts {
			t.Fatalf("dl[%d]=%+v, want offset=%d group=late attempts=%d", i, dl, off, cfg.Retry.MaxAttempts)
		}
		if dl.Event.ID != fmt.Sprintf("dl-0-%d", off) || string(dl.Event.Payload) != fmt.Sprintf("h%02d", off) {
			t.Fatalf("dl[%d] event not restored from disk log: %+v", i, dl.Event)
		}
		if !strings.Contains(dl.Reason, fmt.Sprintf("history boom at offset %d", off)) {
			t.Fatalf("dl[%d] reason=%q, want cause", i, dl.Reason)
		}
		if dl.FailedAt.IsZero() {
			t.Fatalf("dl[%d] FailedAt not set", i)
		}
	}
	if earlyDL, err := b.DeadLetters("dl", "early"); err != nil || len(earlyDL) != 0 {
		t.Fatalf("early group dead letters=%v err=%v, want none (no cross-group routing)", earlyDL, err)
	}

	// 投递语义：耗尽事件恰好 MaxAttempts 次（不无限重试），offset1 退避重投后成功，
	// 其余恰好 1 次；严格按 offset 顺序，死信后续事件（含实时新事件）照常消费。
	lateMu.Lock()
	wantAttempts := map[uint64]int{0: 3, 1: 2, 2: 1, 3: 3, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1}
	for off, want := range wantAttempts {
		if got := attemptCnt[off]; got != want {
			t.Fatalf("offset %d attempts=%d, want %d (attemptCnt=%v)", off, got, want, attemptCnt)
		}
	}
	wantOrder := []uint64{0, 0, 0, 1, 1, 2, 3, 3, 3, 4, 5, 6, 7, 8, 9}
	if fmt.Sprint(deliveryOrder) != fmt.Sprint(wantOrder) {
		t.Fatalf("delivery order=%v, want %v", deliveryOrder, wantOrder)
	}
	lateMu.Unlock()

	// 全局计数：早到组 10 投 10 确认；晚到组 15 投、8 确认、5 次重试、2 死信；
	// 无重复投递、无误拒发布。
	st := b.Stats()
	wantStats := Stats{Published: total, Delivered: 25, Acked: 18, Retried: 5, DeadLettered: 2}
	if st.Published != wantStats.Published || st.Delivered != wantStats.Delivered ||
		st.Acked != wantStats.Acked || st.Retried != wantStats.Retried ||
		st.DeadLettered != wantStats.DeadLettered || st.Duplicates != 0 || st.Rejected != 0 {
		t.Fatalf("stats=%+v, want %+v (Duplicates=0 Rejected=0)", st, wantStats)
	}
	t.Logf("[阶段4] 补历史完成：late.committed=%d early.committed=%d；Backlog=%+v；"+
		"内存窗口 base=%d len=%d；DLQ(late)=%d；Stats=%+v",
		p.committedOffset("late"), p.committedOffset("early"), b.Backlog()[0],
		mustBase(p), mustLen(p), len(dls), st)

	cancel()
	lateWG.Wait()
	earlyWG.Wait()
	for _, e := range lateErrs {
		t.Fatalf("late consumer unexpected error: %v", e)
	}
	for _, e := range earlyErrs {
		t.Fatalf("early consumer unexpected error: %v", e)
	}
}

func mustBase(p *partition) uint64 {
	b, _ := p.memWindow()
	return b
}

func mustLen(p *partition) int {
	_, n := p.memWindow()
	return n
}

// TestLateSubscriberHistoryMiddleDeadLetterNoFalseBackpressure 死信落在历史中间
// （不是第一条），且组落后很多、内存一条历史不剩：进程不崩、位点越过中间死信继续
// 推进；同时字节容量上限只按最慢组真实未确认积压判定，补历史期间实时发布不得被
// 高估的 PayloadBytes 误拒。
func TestLateSubscriberHistoryMiddleDeadLetterNoFalseBackpressure(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Retry.MaxAttempts = 2
	cfg.MaxMemoryBytes = 64 // 大于补历史期间真实最大积压（12 条 × 3 字节）；
	// 旧实现把整段已确认逐出前缀计入 PayloadBytes 会把积压高估到 72 字节而误拒发布。
	cfg.BackpressurePolicy = BackpressureReject
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("mid", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := b.topics["mid"].partitions[0]

	const hist = 8
	early, err := b.Subscribe("mid", "early", "c0")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < hist; i++ {
		if _, _, err := b.Publish(ctx, "mid", "k", []byte(fmt.Sprintf("h%02d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	for i := 0; i < hist; i++ {
		ev, err := early.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := early.Ack(ev); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		base, n := p.memWindow()
		return base == hist && n == 0
	}, "memory empty before late join")

	var mu sync.Mutex
	errs := []error{}
	var wg sync.WaitGroup
	late, err := b.Subscribe("mid", "late", "c1")
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go serve(ctx, b, late, Handler(func(ctx context.Context, e Event) error {
		if e.Offset == 2 {
			return errors.New("middle history always fails")
		}
		return nil
	}), &wg, &errs, &mu)

	// 晚到组落后 hist 条、内存零历史时持续发布实时事件：
	// 若 PayloadBytes 把已确认的逐出前缀整段计入，积压会被高估到远超 12 字节上限，
	// 发布必被误拒；修复后只算晚到组真实未确认部分。
	const live = 4
	for i := 0; i < live; i++ {
		if _, _, err := b.Publish(ctx, "mid", "k", []byte(fmt.Sprintf("n%02d", i))); err != nil {
			t.Fatalf("live publish %d must not be rejected by inflated byte backlog: %v", i, err)
		}
	}

	waitFor(t, func() bool { return b.Stats().DeadLettered == 1 }, "middle history dead-lettered")
	waitFor(t, func() bool { return p.committedOffset("late") == hist+live }, "late group catches up past middle dead letter")
	if b.Stats().Rejected != 0 {
		t.Fatalf("Rejected=%d, want 0 (byte cap must not misfire during history replay)", b.Stats().Rejected)
	}
	dls, err := b.DeadLetters("mid", "late")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Event.Offset != 2 || dls[0].Attempts != cfg.Retry.MaxAttempts {
		t.Fatalf("dead letters=%+v, want single offset 2 attempts %d", dls, cfg.Retry.MaxAttempts)
	}
	if !strings.Contains(dls[0].Reason, "middle history always fails") {
		t.Fatalf("dead letter reason=%q", dls[0].Reason)
	}
	t.Logf("判定依据: 历史 offset 2（非首条、内存已腾空、落后 %d 条）耗尽后死信，"+
		"late.committed=%d 越过中间死信；实时发布 %d 条零误拒 Rejected=%d；Backlog=%+v；Stats=%+v",
		hist, p.committedOffset("late"), live, b.Stats().Rejected, b.Backlog()[0], b.Stats())

	cancel()
	wg.Wait()
	for _, e := range errs {
		t.Fatalf("consumer error: %v", e)
	}
}
