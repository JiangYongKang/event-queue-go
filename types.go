package queue

import "time"

// BackpressurePolicy 决定积压达到上限时的行为。
type BackpressurePolicy int

const (
	// BackpressureBlock 阻塞发布直到有空间或上下文取消。
	BackpressureBlock BackpressurePolicy = iota
	// BackpressureReject 立即拒绝发布并返回 ErrBackpressure。
	BackpressureReject
)

// RetryPolicy 描述失败重试与退避策略。
type RetryPolicy struct {
	MaxAttempts    int           // 最大投递尝试次数（含首次），耗尽后进入死信
	InitialBackoff time.Duration // 首次重试退避
	MaxBackoff     time.Duration // 退避上限
	Multiplier     float64       // 每次重试的退避倍增系数
}

// Config 为 Broker 配置。
type Config struct {
	DataDir                 string             // 本地落盘目录
	NumPartitions           int                // 每个主题的默认分区数
	MaxBacklogPerPartition  int                // 单分区最大未确认积压（事件数）
	MaxMemoryBytes          int64              // 单分区最大内存占用（payload 字节）
	BackpressurePolicy      BackpressurePolicy // 积压策略
	MaxInFlightPerPartition int                // 单分区最大并行在途事件数
	Retry                   RetryPolicy        // 重试策略
}

// Event 为队列中的事件。
type Event struct {
	ID        string
	Topic     string
	Partition int
	Offset    uint64
	Key       string
	Payload   []byte
	Timestamp time.Time
	Attempts  int // 已投递次数（含本次）
}

// DeadLetter 为死信记录，携带可查询的失败原因。
type DeadLetter struct {
	Event    Event
	Group    string
	Reason   string
	Attempts int
	FailedAt time.Time
}

// Stats 为可观测统计（计数器均为原子操作）。
type Stats struct {
	Published    uint64
	Delivered    uint64
	Acked        uint64
	Retried      uint64
	DeadLettered uint64
	Duplicates   uint64 // 被幂等去重拦截的重复投递
	Rejected     uint64 // 被背压拒绝的发布
}

// BacklogMetrics 为单分区积压指标快照。
type BacklogMetrics struct {
	Topic        string
	Partition    int
	Pending      int   // 已落盘未投递
	InFlight     int   // 已投递未确认
	PayloadBytes int64 // 未确认事件的 payload 字节数
}
