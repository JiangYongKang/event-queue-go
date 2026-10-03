package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// counters 为全局原子计数器。
type counters struct {
	published    atomic.Uint64
	delivered    atomic.Uint64
	acked        atomic.Uint64
	retried      atomic.Uint64
	deadLettered atomic.Uint64
	duplicates   atomic.Uint64
	rejected     atomic.Uint64
}

// groupSink 由消费组实现，分区派发器通过它把事件投递给正在 Poll 的消费者。
type groupSink interface {
	groupName() string
	// deliver 阻塞直到事件被某个消费者取走，或 stop 被关闭（返回 false）。
	deliver(ev Event, stop <-chan struct{}) bool
}

// inflight 记录在途事件的投递版本号，用于识别陈旧确认（位点回退）。
type inflight struct {
	attempt int
}

// partGroup 为“分区 × 消费组”维度的投递状态。
type partGroup struct {
	sink      groupSink
	committed uint64              // 已连续确认/死信跳过的位点（offset < committed 不会再投递）
	acked     map[uint64]struct{} // 已确认但尚未连续的位点
	dead      map[uint64]struct{} // 已进入死信、视为终态的位点
	inFlight  map[uint64]inflight // 在途位点 -> 当前投递版本
	attempts  map[uint64]int      // 各位点累计投递次数
	retryAt   map[uint64]time.Time
	wake      chan struct{}
}

func newPartGroup(sink groupSink, committed uint64) *partGroup {
	return &partGroup{
		sink:      sink,
		committed: committed,
		acked:     map[uint64]struct{}{},
		dead:      map[uint64]struct{}{},
		inFlight:  map[uint64]inflight{},
		attempts:  map[uint64]int{},
		retryAt:   map[uint64]time.Time{},
		wake:      make(chan struct{}, 1),
	}
}

func (pg *partGroup) signal() {
	select {
	case pg.wake <- struct{}{}:
	default:
	}
}

// partition 维护单个分区的全部事件与有序投递状态。
// offset 为全局位点，从 0 开始严格按发布顺序递增；事件日志只增不改地落在磁盘。
// 内存中只保留 [base, total) 的滑动窗口：events[0] 的位点即 base；
// 位点 < base 的事件已被所有已知消费组确认，从内存逐出（磁盘仍在），
// 晚到的消费组需要时按位点从磁盘回读，投递语义不变。
type partition struct {
	topic string
	index int
	cfg   Config
	store *fileStore
	cnt   *counters

	mu     sync.Mutex
	cond   *sync.Cond // 发布者背压等待
	base   uint64     // 内存窗口起点：events[0] 的位点
	events []Event    // 内存窗口，覆盖位点 [base, base+len(events))
	groups map[string]*partGroup

	evictedBytes int64 // 已逐出事件（位点 < base）的 payload 总字节，用于积压字节核算

	restored map[string]uint64 // 启动恢复出的 group -> committed
	closed   bool
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func newPartition(topic string, index int, cfg Config, store *fileStore, cnt *counters, restored map[string]uint64) *partition {
	p := &partition{
		topic:    topic,
		index:    index,
		cfg:      cfg,
		store:    store,
		cnt:      cnt,
		groups:   map[string]*partGroup{},
		restored: restored,
		stopCh:   make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// restore 加载落盘事件，必须在启动任何派发器之前调用。
// 加载后立即按恢复出的确认水位逐出前缀：重启不放大常驻内存，
// 累计发布量远大于配置上限时，内存占用仍只与上限相关。
func (p *partition) restore(events []Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = events
	p.evictLocked()
}

// totalLocked 返回下一个待分配的位点（= 分区累计发布数）。
func (p *partition) totalLocked() uint64 {
	return p.base + uint64(len(p.events))
}

// ensureGroup 惰性创建消费组状态并启动其有序派发器。
func (p *partition) ensureGroup(sink groupSink) *partGroup {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pg, ok := p.groups[sink.groupName()]; ok {
		return pg
	}
	committed := p.restored[sink.groupName()]
	if committed > p.totalLocked() {
		committed = p.totalLocked()
	}
	pg := newPartGroup(sink, committed)
	p.groups[sink.groupName()] = pg
	p.wg.Add(1)
	go p.dispatchLoop(pg)
	return pg
}

// ackHorizonLocked 返回所有已知消费组中最小的已确认位点（确认水位）。
// 无任何消费组（含恢复出的）时水位为 0：所有事件均未确认，
// 背压与积压指标不因为当下没有订阅者而失效。
func (p *partition) ackHorizonLocked() uint64 {
	var horizon uint64
	first := true
	for _, pg := range p.groups {
		if first || pg.committed < horizon {
			horizon = pg.committed
			first = false
		}
	}
	// 尚未实例化但恢复出位点的组同样参与背压计算。
	for g, off := range p.restored {
		if _, ok := p.groups[g]; ok {
			continue
		}
		if first || off < horizon {
			horizon = off
			first = false
		}
	}
	return horizon
}

// evictLocked 逐出所有已知消费组都已确认的事件（位点 < 确认水位）。
// 纯容量控制：事件仍在磁盘日志中，位点与确认簿记不受影响；
// 晚到的消费组可从磁盘回读这段历史。在途事件位点 >= 水位，绝不会被逐出。
func (p *partition) evictLocked() {
	horizon := p.ackHorizonLocked()
	if horizon <= p.base {
		return
	}
	n := int(horizon - p.base)
	if n > len(p.events) {
		n = len(p.events)
	}
	for i := 0; i < n; i++ {
		p.evictedBytes += int64(len(p.events[i].Payload))
	}
	// 拷贝到新切片，释放旧底层数组，常驻内存不随累计发布量增长。
	rest := make([]Event, len(p.events)-n)
	copy(rest, p.events[n:])
	p.events = rest
	p.base += uint64(n)
}

// backlogLocked 计算未确认事件数与 payload 字节占用（相对最慢消费组）。
// 已逐出但未确认（仅“全新消费组从 0 订阅”一种情形）的部分用 evictedBytes 补足，
// 指标如实反映积压，与事件是否在内存无关。
func (p *partition) backlogLocked() (int, int64) {
	start := p.ackHorizonLocked()
	total := p.totalLocked()
	if start >= total {
		return 0, 0
	}
	var bytes int64
	from := start
	if from < p.base {
		// start 必为 0（其余情形的 committed 都不小于 base）：逐出部分全部未确认。
		bytes = p.evictedBytes
		from = p.base
	}
	for _, e := range p.events[from-p.base:] {
		bytes += int64(len(e.Payload))
	}
	return int(total - start), bytes
}

// publish 将已构造好的事件持久化并追加到分区，受背压策略约束。
// 先落盘后更新内存：只有持久化成功的事件才可能被投递。
func (p *partition) publish(ctx context.Context, e Event) (uint64, error) {
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	defer stop()

	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return 0, ErrClosed
		}
		n, bytes := p.backlogLocked()
		overN := p.cfg.MaxBacklogPerPartition > 0 && n >= p.cfg.MaxBacklogPerPartition
		overB := p.cfg.MaxMemoryBytes > 0 && bytes >= p.cfg.MaxMemoryBytes
		if overN || overB {
			p.cnt.rejected.Add(1)
			if p.cfg.BackpressurePolicy == BackpressureReject {
				return 0, ErrBackpressure
			}
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			p.cond.Wait() // 被确认推进/停机/ctx 取消唤醒后重新评估
			continue
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		e.Offset = p.totalLocked()
		e.Partition = p.index
		e.Topic = p.topic
		// 确定性事件 ID：绑定 (主题,分区,位点)，全局唯一且重启后不变，供消费者幂等去重。
		e.ID = p.topic + "-" + itoa(p.index) + "-" + formatUint(e.Offset)
		if err := p.store.appendEvent(e); err != nil {
			return 0, err // 已是包装后的对外错误
		}
		p.events = append(p.events, e)
		p.cnt.published.Add(1)
		for _, pg := range p.groups {
			pg.signal()
		}
		return e.Offset, nil
	}
}

// dispatchPlan 为派发决策（持锁计算）。
type dispatchPlan struct {
	off     uint64 // 候选位点
	attempt int
	wait    time.Duration // >0：需等待退避；-1：等待新事件/确认信号
	full    bool          // 在途窗口已满
}

// planLocked 计算下一次派发动作；严格按 offset 顺序，队首未解决前不会越过它。
func (p *partition) planLocked(pg *partGroup, now time.Time) dispatchPlan {
	maxFlight := p.cfg.MaxInFlightPerPartition
	if maxFlight <= 0 {
		maxFlight = 1
	}
	if len(pg.inFlight) >= maxFlight {
		off := pg.committed
		for _, ok := pg.dead[off]; ok; _, ok = pg.dead[off] {
			off++
		}
		return dispatchPlan{off: off, full: true}
	}
	for off := pg.committed; off < p.totalLocked(); off++ {
		if _, dead := pg.dead[off]; dead {
			continue
		}
		if _, flying := pg.inFlight[off]; flying {
			return dispatchPlan{off: off, full: true}
		}
		if t, waiting := pg.retryAt[off]; waiting && t.After(now) {
			return dispatchPlan{off: off, wait: t.Sub(now)}
		}
		return dispatchPlan{off: off, attempt: pg.attempts[off] + 1}
	}
	return dispatchPlan{wait: -1}
}

// dispatchLoop 是“分区 × 组”唯一的有序派发协程。
func (p *partition) dispatchLoop(pg *partGroup) {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		plan := p.planLocked(pg, time.Now())
		switch {
		case plan.wait == -1:
			p.mu.Unlock()
		case plan.wait > 0:
			timer := time.NewTimer(plan.wait)
			p.mu.Unlock()
			select {
			case <-pg.wake:
			case <-timer.C:
			case <-p.stopCh:
				timer.Stop()
				return
			}
			timer.Stop()
			continue
		case plan.full:
			p.mu.Unlock()
		default:
			// 位点 < base 的历史事件已被逐出内存（消费端晚到场景），
			// 先完成在途簿记，解锁后从磁盘日志回读，投递语义与内存命中完全一致。
			fromDisk := plan.off < p.base
			var ev Event
			if !fromDisk {
				ev = p.events[plan.off-p.base]
			}
			ev.Attempts = plan.attempt
			pg.inFlight[plan.off] = inflight{attempt: plan.attempt}
			pg.attempts[plan.off] = plan.attempt
			delete(pg.retryAt, plan.off)
			if plan.attempt > 1 {
				p.cnt.retried.Add(1)
			}
			p.cnt.delivered.Add(1)
			p.mu.Unlock()

			if fromDisk {
				loaded, err := p.store.readEvent(p.topic, p.index, plan.off)
				if err != nil {
					// 回读失败不消耗投递次数：回滚簿记并退避重试，事件不会丢失。
					p.mu.Lock()
					delete(pg.inFlight, plan.off)
					pg.attempts[plan.off] = plan.attempt - 1
					pg.retryAt[plan.off] = time.Now().Add(backoffFor(p.cfg.Retry, plan.attempt))
					p.cnt.delivered.Add(^uint64(0))
					if plan.attempt > 1 {
						p.cnt.retried.Add(^uint64(0))
					}
					p.mu.Unlock()
					continue
				}
				ev = loaded
				ev.Attempts = plan.attempt
			}
			if !pg.sink.deliver(ev, p.stopCh) {
				return
			}
			continue
		}
		select {
		case <-pg.wake:
		case <-p.stopCh:
			return
		}
	}
}

// advanceLocked 将连续的已确认/死信前缀推进为 committed。
// 先持久化位点，成功后才更新内存簿记，保证不存在半提交状态；
// 持久化失败时返回 (committed, false)，调用方负责回滚本轮簿记改动。
func (p *partition) advanceLocked(pg *partGroup) (uint64, bool) {
	target := pg.committed
	for {
		_, isAck := pg.acked[target]
		_, isDead := pg.dead[target]
		if !isAck && !isDead {
			break
		}
		target++
	}
	if target == pg.committed {
		return target, false
	}
	all := map[string]uint64{}
	for g, off := range p.restored {
		if _, live := p.groups[g]; !live {
			all[g] = off
		}
	}
	for g, g2 := range p.groups {
		all[g] = g2.committed
	}
	if err := p.store.commitOffset(p.topic, p.index, pg.sink.groupName(), target, all); err != nil {
		return pg.committed, false
	}
	for pg.committed < target {
		delete(pg.acked, pg.committed)
		delete(pg.dead, pg.committed)
		delete(pg.attempts, pg.committed)
		delete(pg.retryAt, pg.committed)
		pg.committed++
	}
	return target, true
}

// ack 处理确认，返回确认后的连续位点。
func (p *partition) ack(group string, e Event) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pg, ok := p.groups[group]
	if !ok {
		return 0, ErrUnknownGroup
	}
	if e.Offset >= p.totalLocked() {
		return 0, ErrUnknownEvent
	}
	st, flying := pg.inFlight[e.Offset]
	if !flying {
		if e.Offset < pg.committed {
			return pg.committed, ErrDuplicateAck
		}
		if _, done := pg.acked[e.Offset]; done {
			return pg.committed, ErrDuplicateAck
		}
		if _, dead := pg.dead[e.Offset]; dead {
			return pg.committed, ErrDuplicateAck
		}
		return pg.committed, ErrUnknownEvent
	}
	if st.attempt != e.Attempts {
		return pg.committed, ErrOffsetRegression
	}
	delete(pg.inFlight, e.Offset)
	delete(pg.retryAt, e.Offset)
	pg.acked[e.Offset] = struct{}{}
	p.cnt.acked.Add(1)
	if _, ok := p.advanceLocked(pg); !ok && pg.committed == e.Offset {
		// 位点持久化失败，且本事件正位于待提交队首：回滚簿记，
		// 事件恢复为在途（至少一次语义），消费者可重试确认。
		delete(pg.acked, e.Offset)
		pg.inFlight[e.Offset] = inflight{attempt: e.Attempts}
		pg.attempts[e.Offset] = e.Attempts
		p.cnt.acked.Add(^uint64(0))
		return e.Offset, ErrPersistence
	}
	p.evictLocked()  // 已确认前缀释放内存（纯容量控制，不影响位点）
	p.cond.Broadcast() // 释放背压中的发布者
	pg.signal()        // 队首腾出，唤醒派发器
	return pg.committed, nil
}

// nack 处理失败：记录退避重试；次数耗尽则写入死信并携带原因。
func (p *partition) nack(group string, e Event, cause error) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pg, ok := p.groups[group]
	if !ok {
		return 0, ErrUnknownGroup
	}
	if e.Offset >= p.totalLocked() {
		return 0, ErrUnknownEvent
	}
	st, flying := pg.inFlight[e.Offset]
	if !flying {
		_, doneAck := pg.acked[e.Offset]
		_, doneDead := pg.dead[e.Offset]
		if e.Offset < pg.committed || doneAck || doneDead {
			return pg.committed, ErrDuplicateAck
		}
		return pg.committed, ErrUnknownEvent
	}
	if st.attempt != e.Attempts {
		return pg.committed, ErrOffsetRegression
	}
	attempts := pg.attempts[e.Offset]
	delete(pg.inFlight, e.Offset)

	maxAttempts := p.cfg.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if attempts >= maxAttempts {
		pg.dead[e.Offset] = struct{}{}
		reason := "retries exhausted after " + itoa(attempts) + " attempts"
		if cause != nil {
			reason += ": " + cause.Error()
		}
		dl := DeadLetter{
			Event:    p.events[e.Offset-p.base], // 在途事件位点 >= 水位，必在内存窗口内
			Group:    group,
			Reason:   reason,
			Attempts: attempts,
			FailedAt: time.Now(),
		}
		dl.Event.Attempts = attempts
		if err := p.store.appendDeadLetter(dl); err != nil {
			// 死信必须可查：落盘失败则回滚簿记，让消费者可以再次 Nack 触发。
			delete(pg.dead, e.Offset)
			pg.inFlight[e.Offset] = st
			return pg.committed, err
		}
		p.cnt.deadLettered.Add(1)
		committed, _ := p.advanceLocked(pg)
		p.evictLocked()
		p.cond.Broadcast()
		pg.signal()
		return committed, nil
	}
	pg.retryAt[e.Offset] = time.Now().Add(backoffFor(p.cfg.Retry, attempts))
	pg.signal()
	return pg.committed, nil
}

// metrics 返回单分区积压指标快照。
func (p *partition) metrics() BacklogMetrics {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, bytes := p.backlogLocked()
	var inFlight int
	for _, pg := range p.groups {
		inFlight += len(pg.inFlight)
	}
	return BacklogMetrics{
		Topic:        p.topic,
		Partition:    p.index,
		Pending:      n,
		InFlight:     inFlight,
		PayloadBytes: bytes,
	}
}

// committedOffset 返回某组当前连续确认位点（测试与日志用）。
func (p *partition) committedOffset(group string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pg, ok := p.groups[group]; ok {
		return pg.committed
	}
	return p.restored[group]
}

// memWindow 返回内存窗口的起点位点与长度（测试与诊断用）。
func (p *partition) memWindow() (base uint64, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.base, len(p.events)
}

// closePartition 优雅停机：停止派发，未确认事件留在日志中，重启后重新投递。
func (p *partition) closePartition() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cond.Broadcast()
	close(p.stopCh)
	p.mu.Unlock()
	p.wg.Wait()
}
