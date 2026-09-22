package eventqueue

import "time"

// Event 是队列中投递的不可变事件。
type Event struct {
	ID        string
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Payload   []byte
	CreatedAt time.Time
}

// RetryPolicy 描述消费失败后的重试与退避策略。
type RetryPolicy struct {
	MaxRetries  int           // 最大重试次数（不含首次投递）
	BaseBackoff time.Duration // 初始退避
	MaxBackoff  time.Duration // 退避上限
	Multiplier  float64       // 退避倍率
}

// BackpressureMode 发布背压策略。
type BackpressureMode int

const (
	BackpressureBlock  BackpressureMode = iota // 阻塞发布直到有空位
	BackpressureReject                         // 立即拒绝发布
)

// BackpressureConfig 背压配置。
type BackpressureConfig struct {
	MaxPartitionLag int64            // 单分区未确认事件数上限
	Mode            BackpressureMode // 达到上限时的行为
}

// TopicConfig 主题配置。
type TopicConfig struct {
	Name         string
	PartitionNum int
	Backpressure BackpressureConfig
	Retry        RetryPolicy
}

// Config Broker 配置。
type Config struct {
	DataDir       string
	Topics        []TopicConfig
	MaxMemorySize int64 // 全部分区在途/未确认事件载荷的内存占用软上限（字节）
}

// DLQRecord 死信记录，携带可查询的失败原因。
type DLQRecord struct {
	EventID   string
	Topic     string
	Partition int
	Offset    int64
	Attempts  int
	Reason    string
	DeadAt    time.Time
}

// Handler 业务消费回调。返回 error 视为消费失败，触发重试；nil 视为成功确认。
type Handler func(ev *Event) error

// Stats 可观测指标快照。
type Stats struct {
	Published       int64
	Delivered       int64
	Acked           int64
	Redelivered     int64
	DuplicateCalls  int64 // 幂等包装器识别出的重复投递次数（计入正常投递而非失败）
	Failed          int64
	DeadLettered    int64
	InFlight        int64
	Lag             int64
	MemoryBytes     int64
	RejectedPublish int64
	BlockedPublish  int64
	ByPartition     map[string]PartitionStats
}

// PartitionStats 单分区指标。
type PartitionStats struct {
	NextOffset int64
	Committed  int64
	InFlight   int64
	Lag        int64
}
