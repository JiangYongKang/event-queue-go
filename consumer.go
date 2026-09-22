package eventqueue

// Consumer 消费者组订阅句柄。业务可以使用 handler 自动确认模式，
// 也可以在 handler 中保存 *Event，稍后通过 Ack/Nack 手动确认。
type Consumer struct {
	broker  *Broker
	group   string
	topic   string
	handler Handler
}

func newConsumer(b *Broker, group, topic string, h Handler) *Consumer {
	return &Consumer{broker: b, group: group, topic: topic, handler: h}
}

// Ack 显式确认事件。重复确认、未知事件、位点回退都会返回可区分错误。
func (c *Consumer) Ack(ev *Event) error {
	return c.broker.Ack(c.group, ev)
}

// Nack 显式否认事件，按主题重试策略退避重试；耗尽后进入死信。
func (c *Consumer) Nack(ev *Event, cause error) error {
	return c.broker.Nack(c.group, ev, cause)
}

// Group 返回消费者组名。
func (c *Consumer) Group() string { return c.group }

// Topic 返回订阅的主题名。
func (c *Consumer) Topic() string { return c.topic }
