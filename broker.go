package queue

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// topic 持有一个主题的所有分区及其消费组。
type topic struct {
	name       string
	partitions []*partition
	groups     map[string]*consumerGroup
}

// Broker 提供主题/分区/消费组的发布订阅入口。
type Broker struct {
	cfg    Config
	store  *fileStore
	cnt    counters
	stopCh chan struct{}

	mu     sync.Mutex
	topics map[string]*topic
	closed bool
}

// NewBroker 校验配置、打开本地存储并从磁盘恢复全部主题、事件与位点。
func NewBroker(cfg Config) (*Broker, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	st, err := newFileStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:    cfg,
		store:  st,
		topics: map[string]*topic{},
		stopCh: make(chan struct{}),
	}
	if err := b.recover(); err != nil {
		_ = st.close()
		return nil, err
	}
	return b, nil
}

func validateConfig(cfg Config) error {
	if cfg.DataDir == "" {
		return wrapConfig("DataDir is empty")
	}
	if cfg.NumPartitions <= 0 {
		return wrapConfig("NumPartitions must be positive")
	}
	if cfg.MaxInFlightPerPartition < 0 {
		return wrapConfig("MaxInFlightPerPartition must not be negative")
	}
	if cfg.Retry.MaxAttempts <= 0 {
		return wrapConfig("Retry.MaxAttempts must be positive")
	}
	if cfg.Retry.Multiplier < 1 && cfg.Retry.Multiplier != 0 {
		return wrapConfig("Retry.Multiplier must be >= 1")
	}
	return nil
}

func wrapConfig(msg string) error {
	return &publicError{cause: ErrInvalidConfig, msg: msg}
}

// publicError 对外只暴露可区分的哨兵原因，内部细节通过文本保留但不透出内部错误类型。
type publicError struct {
	cause error
	msg   string
}

func (e *publicError) Error() string { return e.cause.Error() + ": " + e.msg }
func (e *publicError) Unwrap() error { return e.cause }

// recover 扫描数据目录恢复主题、分区事件与消费组位点。
func (b *Broker) recover() error {
	offsets, err := b.store.loadOffsets()
	if err != nil {
		return err
	}
	dirs, err := b.store.listTopics()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(dirs))
	for name := range dirs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parts := dirs[name]
		sort.Ints(parts)
		n := len(parts)
		if max := parts[len(parts)-1] + 1; max > n {
			n = max
		}
		if n < b.cfg.NumPartitions {
			n = b.cfg.NumPartitions
		}
		t := &topic{name: name, groups: map[string]*consumerGroup{}}
		for pi := 0; pi < n; pi++ {
			restored := map[string]uint64{}
			for k, off := range offsets {
				if k.Topic == name && k.Partition == pi {
					restored[k.Group] = off
				}
			}
			p := newPartition(name, pi, b.cfg, b.store, &b.cnt, restored)
			// 保留窗口起点取所有已恢复组位点的最小值；没有任何组记录时从 0 起，
			// 保证“先发布、后订阅”的历史事件重启后仍从头可读、可投递。
			var base uint64
			first := true
			for _, off := range restored {
				if first || off < base {
					base = off
					first = false
				}
			}
			// 只重建轻量元数据（位点 + payload 长度），事件体不进内存，
			// 常驻内存因此与累计发布量无关；派发缺页时再按块从日志读回。
			head, sizes, err := b.store.scanPartition(name, pi, base)
			if err != nil {
				return err
			}
			if base > head {
				base = head
			}
			p.restore(base, head, sizes)
			t.partitions = append(t.partitions, p)
			b.cnt.published.Add(head)
		}
		b.topics[name] = t
	}
	return nil
}

// CreateTopic 创建主题；已存在的主题直接返回（幂等）。
func (b *Broker) CreateTopic(name string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if name == "" {
		return wrapConfig("empty topic name")
	}
	if partitions <= 0 {
		partitions = b.cfg.NumPartitions
	}
	if _, ok := b.topics[name]; ok {
		return nil
	}
	t := &topic{name: name, groups: map[string]*consumerGroup{}}
	for i := 0; i < partitions; i++ {
		t.partitions = append(t.partitions,
			newPartition(name, i, b.cfg, b.store, &b.cnt, map[string]uint64{}))
	}
	b.topics[name] = t
	return nil
}

func partitionForKey(t *topic, key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(t.partitions)))
}

// Publish 发布事件：按 Key 哈希选择分区，返回分区号与位点。
// 事件 ID 在单分区内可预测地绑定 (主题,分区,位点)，便于幂等去重与测试复现。
func (b *Broker) Publish(ctx context.Context, topicName, key string, payload []byte) (int, uint64, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, 0, ErrClosed
	}
	t, ok := b.topics[topicName]
	b.mu.Unlock()
	if !ok {
		return 0, 0, ErrUnknownTopic
	}
	pi := partitionForKey(t, key)
	e := Event{
		Topic:     topicName,
		Key:       key,
		Payload:   append([]byte(nil), payload...),
		Timestamp: time.Now(),
	}
	off, err := t.partitions[pi].publish(ctx, e)
	if err != nil {
		return pi, 0, err
	}
	return pi, off, nil
}

// Subscribe 以消费组名义订阅主题；同一组可注册多个消费者并行消费。
func (b *Broker) Subscribe(topicName, group, consumerID string) (*Consumer, error) {
	if group == "" || consumerID == "" {
		return nil, wrapConfig("group and consumerID are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	t, ok := b.topics[topicName]
	if !ok {
		return nil, ErrUnknownTopic
	}
	g, ok := t.groups[group]
	if !ok {
		g = newConsumerGroup(topicName, group)
		t.groups[group] = g
	}
	for _, p := range t.partitions {
		p.ensureGroup(g)
	}
	return &Consumer{
		id:     consumerID,
		group:  group,
		topic:  topicName,
		broker: b,
		stopCh: b.stopCh,
		sink:   g,
	}, nil
}

// Stats 返回计数快照。
func (b *Broker) Stats() Stats {
	return Stats{
		Published:    b.cnt.published.Load(),
		Delivered:    b.cnt.delivered.Load(),
		Acked:        b.cnt.acked.Load(),
		Retried:      b.cnt.retried.Load(),
		DeadLettered: b.cnt.deadLettered.Load(),
		Duplicates:   b.cnt.duplicates.Load(),
		Rejected:     b.cnt.rejected.Load(),
	}
}

// Backlog 返回各分区积压指标。
func (b *Broker) Backlog() []BacklogMetrics {
	b.mu.Lock()
	names := make([]string, 0, len(b.topics))
	for n := range b.topics {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []BacklogMetrics
	for _, n := range names {
		t := b.topics[n]
		for _, p := range t.partitions {
			out = append(out, p.metrics())
		}
	}
	b.mu.Unlock()
	return out
}

// DeadLetters 返回可查询的死信记录，可按主题与消费组过滤（空串表示不过滤）。
func (b *Broker) DeadLetters(topicName, group string) ([]DeadLetter, error) {
	all, err := b.store.loadDeadLetters()
	if err != nil {
		return nil, err
	}
	out := all[:0:0]
	for _, dl := range all {
		if topicName != "" && dl.Event.Topic != topicName {
			continue
		}
		if group != "" && dl.Group != group {
			continue
		}
		out = append(out, dl)
	}
	return out, nil
}

func (b *Broker) partition(topicName string, pi int) (*partition, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[topicName]
	if !ok {
		return nil, ErrUnknownTopic
	}
	if pi < 0 || pi >= len(t.partitions) {
		return nil, ErrUnknownPartition
	}
	return t.partitions[pi], nil
}

// Close 优雅停机：停止全部派发器，已确认位点均已持久化，
// 未确认事件留在分区日志中，下次启动从已提交位点重新投递。
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.stopCh)
	var ps []*partition
	for _, t := range b.topics {
		for _, p := range t.partitions {
			ps = append(ps, p)
		}
	}
	b.mu.Unlock()
	for _, p := range ps {
		p.closePartition()
	}
	return b.store.close()
}
