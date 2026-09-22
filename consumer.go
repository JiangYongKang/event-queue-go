package queue

import "context"

// Handler 处理事件；返回 error 表示处理失败，将按重试策略重投。
type Handler func(ctx context.Context, e Event) error

// Consumer 为订阅句柄。同一消费组内可创建多个 Consumer 并行消费同一分区；
// 分区派发器保证事件按发布顺序进入组通道，确认可乱序到达但位点只按连续前缀推进。
type Consumer struct {
	id     string
	group  string
	topic  string
	broker *Broker
	stopCh chan struct{}
	sink   *consumerGroup
}

// ID 返回消费者标识。
func (c *Consumer) ID() string { return c.id }

// Poll 拉取下一条待投递事件（阻塞直到有事件、Broker 停机或 ctx 取消）。
func (c *Consumer) Poll(ctx context.Context) (Event, error) {
	return c.sink.recv(ctx, c.stopCh)
}

// Ack 确认事件。
//   - 对同一在途事件重复确认返回 ErrDuplicateAck；
//   - 对不存在/未投递事件确认返回 ErrUnknownEvent；
//   - 携带旧投递版本号（位点回退）返回 ErrOffsetRegression。
func (c *Consumer) Ack(e Event) error {
	p, err := c.broker.partition(c.topic, e.Partition)
	if err != nil {
		return err
	}
	_, err = p.ack(c.group, e)
	return err
}

// Nack 标记处理失败：事件按退避策略重投，重试耗尽后进入死信并携带原因。
func (c *Consumer) Nack(e Event, cause error) error {
	p, err := c.broker.partition(c.topic, e.Partition)
	if err != nil {
		return err
	}
	_, err = p.nack(c.group, e, cause)
	return err
}

// NewDedupHandler 包装业务 Handler 实现幂等去重：
// 首次投递执行业务副作用；重复投递跳过副作用直接视为成功（不返回失败），
// 并计入 Stats.Duplicates。业务返回失败时释放幂等令牌，使重试能重新执行副作用。
// 调用方仍需对事件正常 Ack；去重不改变确认语义。
func (b *Broker) NewDedupHandler(store *DedupStore, h Handler) Handler {
	return func(ctx context.Context, e Event) error {
		if !store.Claim(e.ID) {
			b.cnt.duplicates.Add(1)
			return nil
		}
		if err := h(ctx, e); err != nil {
			store.Release(e.ID)
			return err
		}
		return nil
	}
}
