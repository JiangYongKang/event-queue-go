package eventqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// Broker 本地事件队列：负责发布、分区调度、严格顺序投递、
// 重试退避、死信、背压、恢复与优雅停机。
type Broker struct {
	cfg   Config
	store *Store

	mu sync.RWMutex
	// 所有内存态都在 mu 下变更；业务 handler 在锁外执行。
	partitions map[pKey]*partition
	topicCfgs  map[string]TopicConfig
	dlq        []DLQRecord
	memBytes   int64
	closed     bool

	// 订阅：group+"|"+topic -> *subscription
	subs      map[string]*subscription
	subMu     sync.Mutex
	wg        sync.WaitGroup // 在途 handler goroutine
	stopCh    chan struct{}
	closeOnce sync.Once

	// 计数器（在 mu 下更新；Stats 快照也在 mu 下读取，因此无需原子操作）
	published       int64
	delivered       int64
	ackedCnt        int64
	redelivered     int64
	failedCnt       int64
	deadLettered    int64
	rejectedPublish int64
	blockedPublish  int64

	// 背压阻塞发布者在条件变量上等待
	cond *sync.Cond

	// 可注入的调度间隔，默认 20ms
	tickInterval time.Duration

	dup dupRegistry
}

// subscription 记录某消费者组对某主题的订阅。
type subscription struct {
	group   string
	topic   string
	handler Handler
}

// NewBroker 创建一个全新的 Broker（要求 DataDir 为空目录或不存在）。
func NewBroker(cfg Config) (*Broker, error) {
	return openBroker(cfg, false)
}

// Open 打开或恢复 Broker：回放本地日志重建分区、位点与死信。
func Open(cfg Config) (*Broker, error) {
	return openBroker(cfg, true)
}

func openBroker(cfg Config, recoverMode bool) (*Broker, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	store, err := NewStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:          cfg,
		store:        store,
		partitions:   map[pKey]*partition{},
		topicCfgs:    map[string]TopicConfig{},
		subs:         map[string]*subscription{},
		stopCh:       make(chan struct{}),
		tickInterval: 20 * time.Millisecond,
	}
	b.cond = sync.NewCond(&b.mu)
	for _, tc := range cfg.Topics {
		b.topicCfgs[tc.Name] = tc
		for i := 0; i < tc.PartitionNum; i++ {
			key := pKey{topic: tc.Name, partition: i}
			b.partitions[key] = newPartition(key, tc)
		}
	}
	if err := b.recoverFromDisk(); err != nil {
		return nil, err
	}
	go b.dispatchLoop()
	return b, nil
}

// validateConfig 非法配置返回 ErrInvalidConfig，原因写入消息，不暴露内部错误。
func validateConfig(cfg Config) error {
	if cfg.DataDir == "" {
		return newError(ErrInvalidConfig, "validateConfig", "DataDir is required", nil)
	}
	if len(cfg.Topics) == 0 {
		return newError(ErrInvalidConfig, "validateConfig", "at least one topic is required", nil)
	}
	names := map[string]bool{}
	for _, tc := range cfg.Topics {
		if tc.Name == "" {
			return newError(ErrInvalidConfig, "validateConfig", "topic name is required", nil)
		}
		if names[tc.Name] {
			return newError(ErrInvalidConfig, "validateConfig", "duplicate topic: "+tc.Name, nil)
		}
		names[tc.Name] = true
		if tc.PartitionNum <= 0 {
			return newError(ErrInvalidConfig, "validateConfig",
				fmt.Sprintf("topic %s: PartitionNum must be > 0", tc.Name), nil)
		}
		bp := tc.Backpressure
		if bp.MaxPartitionLag < 0 {
			return newError(ErrInvalidConfig, "validateConfig",
				fmt.Sprintf("topic %s: MaxPartitionLag must be >= 0", tc.Name), nil)
		}
		rp := tc.Retry
		if rp.MaxRetries < 0 {
			return newError(ErrInvalidConfig, "validateConfig",
				fmt.Sprintf("topic %s: MaxRetries must be >= 0", tc.Name), nil)
		}
		if rp.MaxRetries > 0 {
			if rp.BaseBackoff < 0 || rp.MaxBackoff < 0 {
				return newError(ErrInvalidConfig, "validateConfig",
					fmt.Sprintf("topic %s: backoff must be >= 0", tc.Name), nil)
			}
			if rp.Multiplier < 1 {
				return newError(ErrInvalidConfig, "validateConfig",
					fmt.Sprintf("topic %s: retry Multiplier must be >= 1", tc.Name), nil)
			}
		}
	}
	if cfg.MaxMemorySize < 0 {
		return newError(ErrInvalidConfig, "validateConfig", "MaxMemorySize must be >= 0", nil)
	}
	return nil
}

// recoverFromDisk 回放事件/ack/死信日志，重建内存态。
// 语义：已确认（含死信）的 offset 重启后不再投递；未确认的 offset 重启后重新投递。
func (b *Broker) recoverFromDisk() error {
	dlq, err := b.store.LoadDLQ()
	if err != nil {
		return newError(ErrInternal, "recover", "load dlq", err)
	}
	b.dlq = dlq
	b.deadLettered = int64(len(dlq))

	keys, err := b.store.ListPartitions()
	if err != nil {
		return newError(ErrInternal, "recover", "list partitions", err)
	}
	for _, key := range keys {
		tc, known := b.topicCfgs[key.topic]
		if !known {
			// 磁盘上存在但配置中已删除的主题：跳过，不污染运行态。
			continue
		}
		if key.partition < 0 || key.partition >= tc.PartitionNum {
			continue
		}
		recs, err := b.store.LoadEvents(key.topic, key.partition)
		if err != nil {
			return newError(ErrInternal, "recover", "load events", err)
		}
		acks, err := b.store.LoadAcks(key.topic, key.partition)
		if err != nil {
			return newError(ErrInternal, "recover", "load acks", err)
		}
		p := b.partitions[key]
		for _, r := range recs {
			ev := &Event{
				ID:        r.id,
				Topic:     key.topic,
				Partition: key.partition,
				Offset:    r.offset,
				Key:       r.key,
				Payload:   r.payload,
				CreatedAt: time.Unix(0, r.createdAt),
			}
			p.events[r.offset] = ev
			if r.offset >= p.nextOffset {
				p.nextOffset = r.offset + 1
			}
		}
		for group, offsets := range acks {
			// 恢复组一律从 offset 0 构建状态：committed 表示“从 0 起的连续确认前缀”。
			// 已确认且被物理驱逐的前缀 offset 不在 ack 集合也不影响判定——
			// 连续性只需结合仍留存的事件窗口验证。
			g := p.getOrCreateGroup(group, 0)
			for _, off := range offsets {
				g.acked[off] = struct{}{}
			}
			g.committed = b.recoveredCommitted(p, g)
		}
		// 恢复阶段不做内存驱逐：此时未来的订阅组尚未建立，若仅因旧组已追平
		// 就驱逐全部历史，重启后新订阅的组将无法从最早事件消费历史。
		// 未确认前缀与全部已发布事件都保留在内存；运行期驱逐由 advanceCommitted
		// 在水位推进时按 retentionWatermark 处理。
	}
	// 重新计算内存与发布计数基线
	b.memBytes = 0
	for _, p := range b.partitions {
		b.memBytes += p.memoryBytes()
	}
	var pub int64
	for _, p := range b.partitions {
		pub += p.nextOffset
	}
	b.published = pub
	return nil
}

// contiguousPrefix 返回集合中从 0 开始的最大连续前缀长度，即 committed 高水位。
func contiguousPrefix(set map[int64]struct{}) int64 {
	var off int64
	for {
		if _, ok := set[off]; !ok {
			return off
		}
		off++
	}
}

// recoveredCommitted 结合“仍留存的事件窗口”与 ack 集合重建连续确认前缀。
// 不在 events 窗口中的小 offset 视为早已确认并驱逐；窗口内的 offset 必须在
// acked 集合中才算连续；一旦遇到既留存又未确认的 offset，前缀即在此中断。
// 无留存事件且有确认记录时，committed 取最大 ack+1（整段历史均已确认）。
func (b *Broker) recoveredCommitted(p *partition, g *groupState) int64 {
	if len(p.events) == 0 {
		var maxAck int64 = -1
		for off := range g.acked {
			if off > maxAck {
				maxAck = off
			}
		}
		if maxAck >= 0 {
			return maxAck + 1
		}
		return 0
	}
	off := int64(0)
	for off < p.nextOffset {
		_, retained := p.events[off]
		if !retained {
			off++ // 已被物理驱逐的确认前缀，跳过
			continue
		}
		if _, ok := g.acked[off]; !ok {
			return off // 第一个留存但未确认的 offset
		}
		off++
	}
	return off
}

// RegisterIdempotent 注册幂等包装器，使其重复投递计数纳入 Broker.Stats。
func (b *Broker) RegisterIdempotent(h *IdempotentHandler) {
	b.dup.register(h)
}

// Publish 发布一条事件。key 非空时按 key 哈希固定分区（同 key 保序），
// key 为空时轮询分区。触发背压时按主题策略阻塞或拒绝。
func (b *Broker) Publish(ctx context.Context, topic, key string, payload []byte) (*Event, error) {
	tc, ok := b.topicCfgs[topic]
	if !ok {
		return nil, newError(ErrUnknownTopic, "Publish", "topic not declared: "+topic, nil)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, newError(ErrBrokerClosed, "Publish", "broker is closed", nil)
	}
	part := 0
	if key != "" {
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		part = int(h.Sum32() % uint32(tc.PartitionNum))
	}
	pk := pKey{topic: topic, partition: part}
	p := b.partitions[pk]

	// 背压判定 + 阻塞等待。位点推进时 cond.Broadcast 唤醒等待者。
	if b.overLimitLocked(tc, p) {
		if tc.Backpressure.Mode == BackpressureReject {
			b.rejectedPublish++
			return nil, newError(ErrBackpressure, "Publish",
				fmt.Sprintf("partition %d lag/memory limit reached", part), nil)
		}
		b.blockedPublish++
		cancelWait := make(chan struct{})
		var waitWg sync.WaitGroup
		waitWg.Add(1)
		go func() {
			defer waitWg.Done()
			select {
			case <-ctx.Done():
				b.cond.Broadcast()
			case <-b.stopCh:
			case <-cancelWait:
			}
		}()
		for b.overLimitLocked(tc, p) {
			b.cond.Wait()
			if b.closed {
				close(cancelWait)
				waitWg.Wait()
				return nil, newError(ErrBrokerClosed, "Publish", "broker closed while waiting", nil)
			}
			if err := ctx.Err(); err != nil {
				close(cancelWait)
				waitWg.Wait()
				return nil, newError(ErrBackpressure, "Publish", "blocked publish cancelled", err)
			}
		}
		close(cancelWait)
		waitWg.Wait()
	}

	off := p.nextOffset
	id := newEventID()
	rec := eventRecord{
		offset:    off,
		id:        id,
		key:       key,
		payload:   payload,
		createdAt: time.Now().UnixNano(),
	}
	// 先持久化并 fsync，再对内存可见：崩溃时最多重复，绝不丢消息。
	if err := b.store.AppendEvent(topic, part, rec); err != nil {
		return nil, newError(ErrInternal, "Publish", "persist event", err)
	}
	ev := &Event{
		ID:        id,
		Topic:     topic,
		Partition: part,
		Offset:    off,
		Key:       key,
		Payload:   append([]byte(nil), payload...),
		CreatedAt: time.Unix(0, rec.createdAt),
	}
	p.events[off] = ev
	p.nextOffset++
	b.published++
	b.memBytes += int64(len(payload))
	return ev, nil
}

// overLimitLocked 判断分区积压或全局内存是否达到上限（0 表示不限制该项）。
func (b *Broker) overLimitLocked(tc TopicConfig, p *partition) bool {
	if tc.Backpressure.MaxPartitionLag > 0 && p.lag() >= tc.Backpressure.MaxPartitionLag {
		return true
	}
	if b.cfg.MaxMemorySize > 0 && b.memBytes >= b.cfg.MaxMemorySize {
		return true
	}
	return false
}

// Subscribe 订阅主题的全部分区。同一 group+topic 重复订阅返回可区分错误。
func (b *Broker) Subscribe(group, topic string, h Handler) (*Consumer, error) {
	if group == "" {
		return nil, newError(ErrInvalidArg, "Subscribe", "empty group", nil)
	}
	if h == nil {
		return nil, newError(ErrInvalidArg, "Subscribe", "nil handler", nil)
	}
	if _, ok := b.topicCfgs[topic]; !ok {
		return nil, newError(ErrUnknownTopic, "Subscribe", "topic not declared: "+topic, nil)
	}
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return nil, newError(ErrBrokerClosed, "Subscribe", "broker is closed", nil)
	}
	b.subMu.Lock()
	defer b.subMu.Unlock()
	key := group + "|" + topic
	if _, exists := b.subs[key]; exists {
		return nil, newError(ErrInvalidArg, "Subscribe", "already subscribed: "+key, nil)
	}
	b.subs[key] = &subscription{group: group, topic: topic, handler: h}

	// 为每个分区建立消费者组状态：
	// 恢复出来的组沿用其 committed（未确认事件重启后继续投递）；
	// 全新组从最早仍留存的事件开始消费（无留存则从队尾开始，不投递不可达的历史）。
	b.mu.Lock()
	tc := b.topicCfgs[topic]
	for i := 0; i < tc.PartitionNum; i++ {
		p := b.partitions[pKey{topic: topic, partition: i}]
		start := p.nextOffset
		for off := range p.events {
			if off < start {
				start = off
			}
		}
		p.getOrCreateGroup(group, start)
	}
	b.mu.Unlock()

	c := newConsumer(b, group, topic, h)
	return c, nil
}

// dispatchLoop 周期驱动全部分区/组的严格顺序投递。
func (b *Broker) dispatchLoop() {
	t := time.NewTicker(b.tickInterval)
	defer t.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-t.C:
			b.tick()
		}
	}
}

func (b *Broker) tick() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	now := time.Now().UnixNano()
	for _, sub := range b.snapshotSubs() {
		tc := b.topicCfgs[sub.topic]
		for i := 0; i < tc.PartitionNum; i++ {
			p := b.partitions[pKey{topic: sub.topic, partition: i}]
			g, ok := p.groups[sub.group]
			if !ok {
				continue
			}
			b.deliverPartition(p, g, sub.handler, now)
		}
	}
}

func (b *Broker) snapshotSubs() []*subscription {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	out := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		out = append(out, s)
	}
	return out
}

// deliverPartition 严格按 offset 顺序串行投递同一个（组, 分区）：
// 只查看队首 offset g.committed ——
//   - 已确认/已死信：推进水位，继续查看下一个；
//   - 在途中：立即返回，绝不向同一分区并发投递第二个事件；
//   - 退避中且未到期：立即返回，后续事件不得越过队首；
//   - 就绪：投递队首后立即返回，等待其终态。
//
// 因此同一分区内的投递与确认顺序严格等于发布顺序，任何情况下都不乱序；
// 并行度来自多分区与多消费者组（不同组各自维护独立队首）。
func (b *Broker) deliverPartition(p *partition, g *groupState, h Handler, now int64) {
	for g.committed < p.nextOffset {
		off := g.committed
		if _, done := g.acked[off]; done {
			// 终止位已在 ack 集合但水位尚未推进（理论上 finishAck 会同步推进，
			// 这里做一次防御性推进）。
			g.committed = contiguousPrefix(g.acked)
			if g.committed == off {
				return
			}
			continue
		}
		if _, busy := g.inflight[off]; busy {
			return // 队首在途，保序等待
		}
		if r, waiting := g.due[off]; waiting && now < r.dueAt {
			return // 队首退避中，保序等待
		}
		ev, ok := p.events[off]
		if !ok {
			return // 未确认前缀事件必须仍在内存；缺失则不跳过（避免乱序/丢失）
		}
		attempt := g.attempts[off] + 1
		if attempt > 1 {
			b.redelivered++
		}
		g.attempts[off] = attempt
		delete(g.due, off)
		g.inflight[off] = &inflightEvent{ev: ev, attempts: attempt}
		b.delivered++
		b.wg.Add(1)
		go b.runHandler(p.key, g.group, ev, attempt, h)
		return // 一次只投队首，等终态后下个 tick 再继续
	}
}

// runHandler 在锁外执行业务 handler，并把结果交回 Broker 终结投递。
// panic 被捕获并视为失败，绝不允许单个消费者崩溃调度器。
func (b *Broker) runHandler(key pKey, group string, ev *Event, attempt int, h Handler) {
	defer b.wg.Done()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("consumer panic: %v", r)
			}
		}()
		err = h(ev)
	}()
	if err == nil {
		_ = b.finishAck(key, group, ev, attempt, nil)
	} else {
		_ = b.finishAck(key, group, ev, attempt, err)
	}
}

// Ack 显式确认。校验失败返回可区分错误，绝不产生位点回退或确认错位。
func (b *Broker) Ack(group string, ev *Event) error {
	if ev == nil {
		return newError(ErrInvalidArg, "Ack", "nil event", nil)
	}
	return b.finishAck(pKey{topic: ev.Topic, partition: ev.Partition}, group, ev, 0, nil)
}

// Nack 显式否认并携带原因（原因进入重试/死信链路，最终可在 DLQ 查询）。
func (b *Broker) Nack(group string, ev *Event, cause error) error {
	if ev == nil {
		return newError(ErrInvalidArg, "Nack", "nil event", nil)
	}
	if cause == nil {
		cause = newError(ErrInvalidArg, "Nack", "nack with nil reason", nil)
	}
	return b.finishAck(pKey{topic: ev.Topic, partition: ev.Partition}, group, ev, 0, cause)
}

// finishAck 终结一次投递。callErr==nil 表示确认，否则表示消费失败。
// attempt 为 0 时（显式 Ack/Nack），以分区记录的在途尝试次数为准。
func (b *Broker) finishAck(key pKey, group string, ev *Event, attempt int, callErr error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return newError(ErrBrokerClosed, "finishAck", "broker closed", nil)
	}
	p, ok := b.partitions[key]
	if !ok {
		return newError(ErrUnknownPart, "finishAck",
			fmt.Sprintf("topic=%s partition=%d", key.topic, key.partition), nil)
	}
	g, ok := p.groups[group]
	if !ok {
		return newError(ErrUnknownEvent, "finishAck", "group never subscribed: "+group, nil)
	}
	off := ev.Offset
	if off < 0 || off >= p.nextOffset {
		return newError(ErrUnknownEvent, "finishAck",
			fmt.Sprintf("offset %d out of range [0,%d)", off, p.nextOffset), nil)
	}
	if _, done := g.acked[off]; done {
		// 已确认（含死信）offset：重复确认必须明确拒绝。
		return newError(ErrDuplicateAck, "finishAck",
			fmt.Sprintf("offset %d already acknowledged, committed=%d", off, g.committed), nil)
	}
	if off < g.committed {
		// committed 水位之前的位点不允许再被确认（位点回退/确认错位）。
		return newError(ErrOffsetRollback, "finishAck",
			fmt.Sprintf("offset %d below committed %d", off, g.committed), nil)
	}
	stored, exists := p.events[off]
	if !exists {
		// 事件前缀已被驱逐（对该组而言早已超过确认水位）：属于未知/过期确认。
		return newError(ErrUnknownEvent, "finishAck",
			fmt.Sprintf("offset %d no longer retained (committed=%d)", off, g.committed), nil)
	}
	if ev.ID != stored.ID {
		return newError(ErrUnknownEvent, "finishAck",
			fmt.Sprintf("event id at offset %d does not match", off), nil)
	}
	in, inflight := g.inflight[off]
	if !inflight {
		// 既未确认也不在途：例如重启后在事件重投之前确认，属于未知/非法确认。
		return newError(ErrUnknownEvent, "finishAck",
			fmt.Sprintf("offset %d is not currently delivered to group %s", off, group), nil)
	}
	if attempt == 0 {
		attempt = in.attempts
	}
	delete(g.inflight, off)

	if callErr != nil {
		return b.handleFailure(p, g, ev, attempt, callErr)
	}

	// 成功确认：先写 ack WAL（fsync），再更新内存可见状态。
	if err := b.store.AppendAck(key.topic, key.partition, group, off); err != nil {
		// 落盘失败：把事件退回在途，避免内存认为已确认而磁盘无记录。
		g.inflight[off] = in
		return newError(ErrInternal, "finishAck", "persist ack", err)
	}
	g.acked[off] = struct{}{}
	b.ackedCnt++
	b.advanceCommitted(p, g)
	return nil
}

// handleFailure 处理消费失败：退避重试；重试次数耗尽则进死信并推进位点。
func (b *Broker) handleFailure(p *partition, g *groupState, ev *Event, attempt int, callErr error) error {
	tc := p.cfg
	b.failedCnt++
	if attempt <= tc.Retry.MaxRetries {
		// 还有重试机会：记录退避到期时间，事件保持未确认，调度器到期重投。
		delay := backoffDelay(tc.Retry, attempt)
		g.due[ev.Offset] = retryEntry{attempts: attempt, dueAt: time.Now().Add(delay).UnixNano()}
		return nil
	}
	// 重试耗尽：死信记录 + 终止位 ack 都先持久化（DLQ 在前、ack 在后），
	// 崩溃在中间也安全：重启后该 offset 未确认会重投，幂等包装器会抑制副作用，
	// 重投再次耗尽时 DLQ 至多重复（按 EventID 去重查询），绝不静默丢弃。
	reason := callErr.Error()
	rec := DLQRecord{
		EventID:   ev.ID,
		Topic:     ev.Topic,
		Partition: ev.Partition,
		Offset:    ev.Offset,
		Attempts:  attempt,
		Reason:    reason,
		DeadAt:    time.Now(),
	}
	if err := b.store.AppendDLQ(rec); err != nil {
		g.inflight[ev.Offset] = &inflightEvent{ev: ev, attempts: attempt}
		return newError(ErrInternal, "handleFailure", "persist dlq", err)
	}
	if err := b.store.AppendAck(ev.Topic, ev.Partition, g.group, ev.Offset); err != nil {
		g.inflight[ev.Offset] = &inflightEvent{ev: ev, attempts: attempt}
		return newError(ErrInternal, "handleFailure", "persist terminal ack", err)
	}
	if _, exists := b.dlqByID(ev.ID); !exists {
		b.dlq = append(b.dlq, rec)
	}
	g.acked[ev.Offset] = struct{}{}
	b.deadLettered++
	b.advanceCommitted(p, g)
	return newError(ErrRetryExhausted, "handleFailure",
		fmt.Sprintf("event %s at offset %d moved to DLQ after %d attempts: %s",
			ev.ID, ev.Offset, attempt, reason), nil)
}

// advanceCommitted 依据连续确认前缀推进高水位，驱逐前缀事件并唤醒背压等待者。
func (b *Broker) advanceCommitted(p *partition, g *groupState) {
	old := g.committed
	g.committed = contiguousPrefix(g.acked)
	if g.committed == old {
		return
	}
	before := p.memoryBytes()
	p.evict()
	b.memBytes += p.memoryBytes() - before
	b.cond.Broadcast()
}

// backoffDelay 指数退避：min(base * multiplier^(attempt-1), max)。
func backoffDelay(p RetryPolicy, attempts int) time.Duration {
	if p.BaseBackoff <= 0 {
		return 0
	}
	d := float64(p.BaseBackoff)
	for i := 1; i < attempts; i++ {
		d *= p.Multiplier
	}
	if p.MaxBackoff > 0 && d > float64(p.MaxBackoff) {
		d = float64(p.MaxBackoff)
	}
	return time.Duration(d)
}

// DLQ 返回全部死信记录快照。
func (b *Broker) DLQ() []DLQRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]DLQRecord, len(b.dlq))
	copy(out, b.dlq)
	return out
}

// DLQByEvent 按事件 ID 查询死信记录及失败原因。
func (b *Broker) DLQByEvent(id string) (DLQRecord, bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	r, ok := b.dlqByID(id)
	return r, ok, nil
}

func (b *Broker) dlqByID(id string) (DLQRecord, bool) {
	for _, r := range b.dlq {
		if r.EventID == id {
			return r, true
		}
	}
	return DLQRecord{}, false
}

// Stats 返回可观测指标快照（位点、在途、积压、内存、投递/重试/死信计数）。
func (b *Broker) Stats() Stats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	st := Stats{
		Published:       b.published,
		Delivered:       b.delivered,
		Acked:           b.ackedCnt,
		Redelivered:     b.redelivered,
		Failed:          b.failedCnt,
		DeadLettered:    b.deadLettered,
		MemoryBytes:     b.memBytes,
		RejectedPublish: b.rejectedPublish,
		BlockedPublish:  b.blockedPublish,
		DuplicateCalls:  b.dup.total(),
		ByPartition:     map[string]PartitionStats{},
	}
	var inflight, lag int64
	for key, p := range b.partitions {
		ps := PartitionStats{NextOffset: p.nextOffset, Committed: p.slowestCommitted()}
		for _, g := range p.groups {
			ps.InFlight += int64(len(g.inflight))
		}
		ps.Lag = p.lag()
		st.ByPartition[fmt.Sprintf("%s:%d", key.topic, key.partition)] = ps
		inflight += ps.InFlight
		lag += ps.Lag
	}
	st.InFlight = inflight
	st.Lag = lag
	return st
}

// Close 优雅停机：立即停止接受新投递与新确认，然后等待在途 handler 完成
// （受 ctx 限制）。停机开始后才完成的 handler 其确认会被拒绝（事件保持未确认，
// 重启后重新投递），从而避免“停机途中确认、与恢复状态竞争”。
func (b *Broker) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true // 先置关闭态：finishAck/Publish/Subscribe 立即开始拒绝
		b.cond.Broadcast()
		b.mu.Unlock()
		close(b.stopCh) // 再通知调度器与监听停机信号的 handler
	})
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}

func newEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
