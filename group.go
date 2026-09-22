package queue

import "context"

// consumerGroup 为一个主题下的消费组，组内多个 Consumer 并行消费：
// 各分区派发器按 offset 顺序把事件投递到本组的共享通道，
// 正在 Poll 的消费者竞争取走事件。单分区内进入通道的顺序即发布顺序。
type consumerGroup struct {
	name  string
	topic string
	ch    chan Event
}

func newConsumerGroup(topic, name string) *consumerGroup {
	return &consumerGroup{
		name:  name,
		topic: topic,
		ch:    make(chan Event),
	}
}

func (g *consumerGroup) groupName() string { return g.name }

// deliver 实现 groupSink：阻塞直到事件被消费者取走或停机。
func (g *consumerGroup) deliver(ev Event, stop <-chan struct{}) bool {
	select {
	case g.ch <- ev:
		return true
	case <-stop:
		return false
	}
}

// recv 供消费者 Poll 使用。
func (g *consumerGroup) recv(ctx context.Context, stop <-chan struct{}) (Event, error) {
	select {
	case ev, ok := <-g.ch:
		if !ok {
			return Event{}, ErrClosed
		}
		return ev, nil
	case <-stop:
		return Event{}, ErrClosed
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}
