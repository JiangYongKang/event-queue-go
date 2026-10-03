package queue

import (
	"context"
	"fmt"
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

// eventChunkSize 为内存缺页时从日志一次按需读回的位点批量。
const eventChunkSize = 64

// partition 维护单个分区的全部事件与有序投递状态。
//
// 绝对位点即追加日志行号，从 0 开始严格按发布顺序递增，全程不重用。
// 内存中只保留一个滑动驻留窗口，常驻内存只与配置上限相关，与累计发布量无关：
//   - base：保留窗口最小位点。base 之前的事件已被所有消费组确认，事件体可淘汰；
//   - head：下一条发布事件的位点（= 日志总行数）；
//   - sizes：[base, head) 内各位点 payload 长度的稠密元数据环，用于无事件体时
//     仍能精确计算积压字节；
//   - events：事件体稀疏驻留，未驻留的位点在派发时从日志按块读回（缺页读盘）。
type partition struct {
	topic string
	index int
	cfg   Config
	store *fileStore
	cnt   *counters

	mu     sync.Mutex
	cond   *sync.Cond // 发布者背压等待
	base   uint64
	head   uint64
	sizes  []int            // 下标 off-base：该位点事件的 payload 字节数
	events map[uint64]Event // 事件体稀疏驻留，缺页时按 eventChunkSize 从日志读入
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
		events:   map[uint64]Event{},
		groups:   map[string]*partGroup{},
		restored: restored,
		stopCh:   make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// restore 重建分区的轻量元数据，必须在启动任何派发器之前调用。
// base 为恢复时的保留窗口起点（所有已恢复位点的最小值），head 为日志总行数，
// sizes 为 [base, head) 的 payload 长度；事件体不进内存，派发缺页时再读盘。
func (p *partition) restore(base, head uint64, sizes []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.base = base
	p.head = head
	p.sizes = sizes
}

// pageInLocked 把从 off 起的一块事件从日志读入内存（调用方持锁）。
// 仅用于派发需要事件体而该位点当前未驻留的场景；读回不改任何位点语义。
func (p *partition) pageInLocked(off uint64) error {
	if off < p.base || off >= p.head {
		return fmt.Errorf("%w: page in offset %d outside window [%d,%d)",
			ErrPersistence, off, p.base, p.head)
	}
	limit := uint64(eventChunkSize)
	if rest := p.head - off; rest < limit {
		limit = rest
	}
	loaded, err := p.store.readEvents(p.topic, p.index, off, limit)
	if err != nil {
		return err
	}
	for _, e := range loaded {
		p.events[e.Offset] = e
	}
	if _, ok := p.events[off]; !ok {
		return fmt.Errorf("%w: event at offset %d missing after read", ErrPersistence, off)
	}
	return nil
}

// eventLocked 返回某位点的事件体，未驻留时按需从日志读回（调用方持锁）。
func (p *partition) eventLocked(off uint64) (Event, error) {
	if ev, ok := p.events[off]; ok {
		return ev, nil
	}
	if err := p.pageInLocked(off); err != nil {
		return Event{}, err
	}
	return p.events[off], nil
}

// ensureGroup 惰性创建消费组状态并启动其有序派发器。
func (p *partition) ensureGroup(sink groupSink) *partGroup {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pg, ok := p.groups[sink.groupName()]; ok {
		return pg
	}
	committed := p.restored[sink.groupName()]
	if committed < p.base {
		committed = p.base
	}
	if committed > p.head {
		committed = p.head
	}
	pg := newPartGroup(sink, committed)
	p.groups[sink.groupName()] = pg
	p.wg.Add(1)
	go p.dispatchLoop(pg)
	return pg
}

// minCommittedLocked 返回所有消费组中最小的已确认位点（决定背压口径）。
// 尚没有任何消费组（含未实例化的恢复组）时返回 base：未被任何组确认的整段
// 保留窗口都算积压，因此“没有订阅者”不会让容量判断失效，也不会让内存无界。
func (p *partition) minCommittedLocked() uint64 {
	min := p.head
	first := true
	consider := func(off uint64) {
		if off < min {
			min = off
		}
		first = false
	}
	for _, pg := range p.groups {
		consider(pg.committed)
	}
	// 尚未实例化但恢复出位点的组同样参与背压计算。
	for g, off := range p.restored {
		if _, ok := p.groups[g]; ok {
			continue
		}
		consider(off)
	}
	if first {
		return p.base
	}
	return min
}

// backlogLocked 计算未确认事件数与 payload 字节占用（相对最慢消费组）。
// 字节统计只依赖 sizes 元数据环，事件体是否驻留内存都不影响准确性。
func (p *partition) backlogLocked() (int, int64) {
	start := p.minCommittedLocked()
	if start < p.base {
		start = p.base
	}
	var bytes int64
	for _, n := range p.sizes[start-p.base:] {
		bytes += int64(n)
	}
	return int(p.head - start), bytes
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
		e.Offset = p.head
		e.Partition = p.index
		e.Topic = p.topic
		// 确定性事件 ID：绑定 (主题,分区,位点)，全局唯一且重启后不变，供消费者幂等去重。
		e.ID = p.topic + "-" + itoa(p.index) + "-" + formatUint(e.Offset)
		if err := p.store.appendEvent(e); err != nil {
			return 0, err // 已是包装后的对外错误
		}
		p.sizes = append(p.sizes, len(e.Payload))
		p.events[e.Offset] = e
		p.head++
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
	for off := pg.committed; off < p.head; off++ {
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
			// 事件体可能已因容量控制被淘汰出内存（如重启后的历史事件），
			// 这里按位点从日志读回；读盘失败不丢事件，短暂退避后在原位点重试。
			ev, perr := p.eventLocked(plan.off)
			if perr != nil {
				timer := time.NewTimer(10 * time.Millisecond)
				p.mu.Unlock()
				select {
				case <-timer.C:
				case <-p.stopCh:
					timer.Stop()
					return
				}
				continue
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
	// 仅在位点持久化并推进内存簿记后，才淘汰所有组都确认的前缀；
	// 淘汰是纯粹的容量控制：日志仍在，重启与缺页读回语义均不变。
	p.evictLocked()
	return target, true
}

// evictLocked 把保留窗口起点推进到所有组的最小已确认位点，
// 释放此前位点的事件体与元数据（纯内存操作，不删除日志，不影响任何组）。
// 调用方须持锁，且只能在内存 committed 成功推进之后调用。
func (p *partition) evictLocked() {
	floor := p.minCommittedLocked()
	if floor < p.base {
		floor = p.base
	}
	if floor <= p.base {
		return
	}
	drop := int(floor - p.base)
	for off := p.base; off < floor; off++ {
		delete(p.events, off)
	}
	if drop >= len(p.sizes) {
		p.sizes = p.sizes[:0]
	} else {
		n := copy(p.sizes, p.sizes[drop:])
		p.sizes = p.sizes[:n]
	}
	p.base = floor
}

// ack 处理确认，返回确认后的连续位点。
func (p *partition) ack(group string, e Event) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pg, ok := p.groups[group]
	if !ok {
		return 0, ErrUnknownGroup
	}
	if e.Offset >= p.head {
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
	if e.Offset >= p.head {
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
		dlEvent, err := p.eventLocked(e.Offset)
		if err != nil {
			// 事件体读不回不能静默吞掉：回滚簿记，消费者可再次 Nack 触发。
			pg.inFlight[e.Offset] = st
			return pg.committed, err
		}
		dl := DeadLetter{
			Event:    dlEvent,
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

// retentionWindow 返回保留窗口 [base, head)（测试与日志用）。
func (p *partition) retentionWindow() (base, head uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.base, p.head
}

// residentEvents 返回当前事件体驻留内存的个数（测试与日志用）。
func (p *partition) residentEvents() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
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
