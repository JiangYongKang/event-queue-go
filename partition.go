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
// offset 即事件在 events 中的下标，从 0 开始，严格按发布顺序递增。
type partition struct {
	topic string
	index int
	cfg   Config
	store *fileStore
	cnt   *counters

	mu     sync.Mutex
	cond   *sync.Cond // 发布者背压等待
	events []Event
	groups map[string]*partGroup

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
func (p *partition) restore(events []Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = events
}

// ensureGroup 惰性创建消费组状态并启动其有序派发器。
func (p *partition) ensureGroup(sink groupSink) *partGroup {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pg, ok := p.groups[sink.groupName()]; ok {
		return pg
	}
	committed := p.restored[sink.groupName()]
	if committed > uint64(len(p.events)) {
		committed = uint64(len(p.events))
	}
	pg := newPartGroup(sink, committed)
	p.groups[sink.groupName()] = pg
	p.wg.Add(1)
	go p.dispatchLoop(pg)
	return pg
}

// minCommittedLocked 返回所有消费组中最小的已确认位点（决定背压口径）。
func (p *partition) minCommittedLocked() uint64 {
	min := uint64(len(p.events))
	first := true
	for _, pg := range p.groups {
		if first || pg.committed < min {
			min = pg.committed
			first = false
		}
	}
	// 尚未实例化但恢复出位点的组同样参与背压计算。
	for g, off := range p.restored {
		if _, ok := p.groups[g]; ok {
			continue
		}
		if first || off < min {
			min = off
			first = false
		}
	}
	return min
}

// backlogLocked 计算未确认事件数与 payload 字节占用（相对最慢消费组）。
func (p *partition) backlogLocked() (int, int64) {
	start := p.minCommittedLocked()
	var bytes int64
	for _, e := range p.events[start:] {
		bytes += int64(len(e.Payload))
	}
	return len(p.events) - int(start), bytes
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
		e.Offset = uint64(len(p.events))
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
	for off := pg.committed; off < uint64(len(p.events)); off++ {
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
			ev := p.events[plan.off]
			ev.Attempts = plan.attempt
			pg.inFlight[plan.off] = inflight{attempt: plan.attempt}
			pg.attempts[plan.off] = plan.attempt
			delete(pg.retryAt, plan.off)
			if plan.attempt > 1 {
				p.cnt.retried.Add(1)
			}
			p.cnt.delivered.Add(1)
			p.mu.Unlock()

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
	if e.Offset >= uint64(len(p.events)) {
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
	if e.Offset >= uint64(len(p.events)) {
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
			Event:    p.events[e.Offset],
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
