package queue

import "errors"

// 对外可区分的错误原因。内部错误一律包装为上述哨兵错误后再返回，
// 不直接透出内部实现细节。
var (
	ErrInvalidConfig    = errors.New("queue: invalid config")
	ErrUnknownTopic     = errors.New("queue: unknown topic")
	ErrUnknownPartition = errors.New("queue: unknown partition")
	ErrOffsetRegression = errors.New("queue: offset regression")
	ErrUnknownEvent     = errors.New("queue: unknown event")
	ErrDuplicateAck     = errors.New("queue: duplicate ack")
	ErrRetriesExhausted = errors.New("queue: retries exhausted")
	ErrBackpressure     = errors.New("queue: backpressure limit reached")
	ErrClosed           = errors.New("queue: broker closed")
	ErrUnknownConsumer  = errors.New("queue: unknown consumer")
	ErrUnknownGroup     = errors.New("queue: unknown consumer group")
	ErrPersistence      = errors.New("queue: local persistence failure")
)
