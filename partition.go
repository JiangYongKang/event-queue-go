package eventqueue

// pKey 全局唯一标识一个分区。
type pKey struct {
	topic     string
	partition int
}

// retryEntry 记录某次失败后最早可再次投递的时间与当前尝试次数。
type retryEntry struct {
	attempts int
	dueAt    int64 // unix nano
}

// inflightEvent 已投递但尚未终态（ack/nack）的事件。
type inflightEvent struct {
	ev       *Event
	attempts int // 本次投递是第几次（1 起）
}

// groupState 是某个消费者组在某个分区上的消费状态。
// 只允许在持有 Broker.mu 时访问（Broker 内所有状态变更加同一把大锁，
// 业务 handler 在锁外执行，因此吞吐不受业务耗时影响）。
type groupState struct {
	group     string
	committed int64 // 连续确认高水位：[0,committed) 已确认（含死信终止位）

	// acked 记录所有已确认/已死信的 offset，允许空洞（乱序确认）。
	acked map[int64]struct{}
	// inflight 是当前已投递、处理中的事件；同一 offset 不会并发投递两次。
	inflight map[int64]*inflightEvent
	// attempts 记录每个 offset 已经历的投递次数。
	attempts map[int64]int
	// due 记录失败后退避中的 offset 及最早可再次投递时间。
	due map[int64]retryEntry
}

func newGroupState(group string, startOffset int64) *groupState {
	return &groupState{
		group:     group,
		committed: startOffset,
		acked:     map[int64]struct{}{},
		inflight:  map[int64]*inflightEvent{},
		attempts:  map[int64]int{},
		due:       map[int64]retryEntry{},
	}
}

// partition 单个分区的内存态。
type partition struct {
	key pKey
	cfg TopicConfig

	// events 保留仍需为至少一个消费者组留存的事件：offset -> Event。
	events map[int64]*Event
	// nextOffset 下一条发布事件的 offset；logEnd 为已发布的最大 offset+1。
	nextOffset int64
	// groups 已订阅该分区的消费者组状态。
	groups map[string]*groupState
}

func newPartition(key pKey, cfg TopicConfig) *partition {
	return &partition{
		key:    key,
		cfg:    cfg,
		events: map[int64]*Event{},
		groups: map[string]*groupState{},
	}
}

func (p *partition) getOrCreateGroup(group string, startOffset int64) *groupState {
	g, ok := p.groups[group]
	if !ok {
		g = newGroupState(group, startOffset)
		p.groups[group] = g
	}
	return g
}

// slowestCommitted 返回所有组中最小的 committed（无组时为 0），
// 用于统计口径与 lag：落后或空闲组都计入最慢水位展示。
func (p *partition) slowestCommitted() int64 {
	if len(p.groups) == 0 {
		return 0
	}
	min := p.nextOffset
	for _, g := range p.groups {
		if g.committed < min {
			min = g.committed
		}
	}
	return min
}

// retentionWatermark 返回事件驱逐水位：
//   - 无订阅组：返回 0（不驱逐，事件等待未来订阅者）；
//   - 有订阅组：返回所有组中最小的 committed。落后组未确认的前缀必须保留；
//     所有组都追平后历史也继续保留在内存（磁盘 WAL 始终保留全部历史），
//     以便后加入的新组从最早事件消费。内存增长由 MaxMemorySize 背压约束，
//     绝不静默删除未过期数据。
func (p *partition) retentionWatermark() int64 {
	min := p.nextOffset
	for _, g := range p.groups {
		if g.committed < min {
			min = g.committed
		}
	}
	return min
}

// lag 返回分区积压：
//   - 无订阅组：全部已发布事件都算积压（等待未来订阅者，内存必须留存）；
//   - 有订阅组：取所有组中最慢组的未确认事件数（空闲组记 0）。
func (p *partition) lag() int64 {
	if len(p.groups) == 0 {
		return p.nextOffset
	}
	return p.nextOffset - p.slowestCommitted()
}

// memoryBytes 返回分区留存事件的载荷字节数（近似内存占用）。
func (p *partition) memoryBytes() int64 {
	var n int64
	for _, ev := range p.events {
		n += int64(len(ev.Payload))
	}
	return n
}

// evict 在水位推进后丢弃所有组都不再需要的事件前缀，防止内存无界增长。
// 没有任何订阅组时不驱逐：事件必须留到至少一个组确认后才可释放。
func (p *partition) evict() {
	if len(p.groups) == 0 {
		return
	}
	min := p.retentionWatermark()
	for off := range p.events {
		if off < min {
			delete(p.events, off)
		}
	}
}
