# 本地事件队列与异步投递系统（event-queue-go）

一个纯本地、零外部依赖的事件队列实现：用本地 WAL 文件模拟消息落盘与恢复，
提供 **主题 / 分区 / 消费者组** 的发布订阅模型，支持严格分区顺序、至少一次投递、
失败重试与退避、死信、背压、优雅停机与重启恢复。

## 1. 核心模型与投递语义

### 1.1 主题、分区、消费者组

- `Topic`：逻辑消息流，由 `TopicConfig` 静态声明，包含分区数、重试策略、背压策略。
- `Partition`：主题内的有序分片。消息发布时：
  - 指定 `key`：对 key 做 FNV-1a 哈希固定分区，**同一 key 永远落在同一分区**；
  - `key` 为空：轮询选择分区。
  - 每条事件在分区内获得单调递增的 `Offset`（从 0 开始）。
- `Consumer Group`：订阅某主题的全部分区。不同组之间相互独立，**同一条事件会被
  每个组各消费一次**；同一组内同一分区严格串行（见下），不同分区并行。

### 1.2 严格分区顺序（不乱序）

对每个 `(group, partition)`，调度器**只投递队首 offset**（即连续确认高水位
`committed`）：

- 队首在途（已投递、未确认）：不投递该分区任何后续事件；
- 队首处于退避等待：后续事件不得越过它；
- 队首确认（成功或死信终止）后，水位才前移并投递下一条。

因此同一分区内，事件的**业务可见顺序与确认顺序严格等于发布顺序**，任何情况下
（重试、重启、并发确认）都不会乱序、越位。并行度来自「多分区 × 多消费者组」。

> 设计权衡：为保证“任何情况下不得乱序”，同一组同一分区不做乱序并行确认。
> 需要高并发时，增加分区数或使用不同消费者组。

### 1.3 至少一次投递 + 幂等去重

- 语义为 **at-least-once**：失败重试、停机恢复都可能让同一事件被投递多次。
- 业务必须幂等。库提供 `IdempotentHandler`，以 `Event.ID` 去重：
  - 同一 ID **业务成功只执行一次**；
  - 重复投递直接返回 `nil`（视为成功），并计入 `Stats.DuplicateCalls`，
    **重复投递是正常情况，绝不计为失败、绝不触发重试**；
  - 业务返回错误时不记忆该 ID，保证重试时业务逻辑仍会执行。

```go
idem := eventqueue.NewIdempotentHandler()
broker.RegisterIdempotent(idem) // 使重复投递计数进入 Stats
handler := idem.Wrap(func(ev *eventqueue.Event) error {
    // 这里的副作用对同一 Event.ID 只会发生一次
    return nil
})
broker.Subscribe("order-workers", "orders", handler)
```

## 2. 失败重试、退避与死信

`RetryPolicy`（按主题配置）：

| 字段 | 含义 |
| --- | --- |
| `MaxRetries` | 最大**重试**次数（不含首次投递）。事件总投递次数 = `MaxRetries + 1` |
| `BaseBackoff` | 首次失败后的退避时长 |
| `MaxBackoff` | 退避上限 |
| `Multiplier` | 退避倍率（必须 ≥ 1） |

- 退避公式：`delay = min(BaseBackoff * Multiplier^(attempt-1), MaxBackoff)`。
- 退避期间事件保持「未确认」状态；到期后由调度器重新投递，仍然排在队首。
- **重试次数耗尽后事件进入死信（DLQ）**，绝不无限重试，也绝不静默丢弃。
- 每条死信记录 `DLQRecord` 携带可查询的失败原因：
  - 字段：`EventID / Topic / Partition / Offset / Attempts / Reason / DeadAt`；
  - 查询：`Broker.DLQ()` 返回全部，`Broker.DLQByEvent(id)` 按事件查询。
- 事件进入死信后，其 offset 写入「终止位」确认，**分区位点继续向后推进**，
  后续事件不会被毒消息永久卡住。
- 业务 handler `panic` 会被捕获并当作一次消费失败处理，不会击垮调度器。
- `Consumer.Nack(ev, cause)` 可手动否认并携带原因；`Consumer.Ack(ev)` 手动确认。

## 3. 位点与确认的并发安全

- 所有分区/组内存状态在单一 `Broker` 互斥锁下变更；业务 handler 始终在锁外执行，
  业务耗时不影响调度吞吐。
- `committed` 是「从 0 起的连续确认前缀」高水位，**只单调递增**：
  - 允许乱序确认：非连续 offset 的确认先记入 `acked` 集合，前缀补齐时水位跳跃前进；
  - 不允许位点回退、越位、确认错位。
- 非法确认被**明确拒绝**，返回可区分错误：

| 场景 | 错误 |
| --- | --- |
| 重复确认（同一 offset 再确认，含并发重复） | `ErrDuplicateAck` |
| 确认未投递/未知 offset、EventID 不匹配、已驱逐 offset | `ErrUnknownEvent` |
| 确认位点低于 committed | `ErrOffsetRollback` |

确认落盘采用「先写 ack WAL 并 fsync，再更新内存可见状态」，落盘失败会把事件
退回在途，杜绝「内存认为已确认、磁盘无记录」的半提交状态。

## 4. 背压策略

`BackpressureConfig`（按主题）与全局内存上限：

- `MaxPartitionLag`：单分区未确认事件数上限（`0` 表示不限制该项）。
- `MaxMemorySize`（全局 `Config`）：全部留存事件载荷的内存占用软上限（字节）。
- `Mode`：
  - `BackpressureBlock`：达到上限时 `Publish` **阻塞**，直到位点推进腾出空间；
    支持通过 `context.Context` 取消等待，停机时自动唤醒。
  - `BackpressureReject`：达到上限时立即返回 `ErrBackpressure`，**不阻塞、不丢消息**。

可观测指标（`Broker.Stats()`）：`Lag / InFlight / MemoryBytes /
RejectedPublish / BlockedPublish / Published / Delivered / Acked /
Redelivered / DuplicateCalls / Failed / DeadLettered`，以及
`ByPartition["topic:partition"]` 的 `NextOffset / Committed / InFlight / Lag`。

内存留存规则：未确认事件以及仍被落后消费者组需要的历史事件必须保留；
运行期在所有相关组都确认后才按保留水位驱逐前缀事件。内存不会无界增长——
达到 `MaxMemorySize` 时通过背压阻塞或拒绝发布，而不是悄悄丢弃。

## 5. 优雅停机与重启恢复

### 5.1 本地存储布局（WAL）

```
<DataDir>/
  events/<topic>-<partition>.log   # 事件 WAL（每分区一个，append-only）
  acks/<topic>-<partition>.log     # 确认 WAL（含死信终止位）
  dlq.log                          # 全局死信日志
```

- 帧格式：`[u64 length][u32 crc32][payload]`，每条记录追加写并 `fsync`。
- 发布顺序：**先持久化事件（fsync）再对内存可见**——崩溃时至多重复，绝不丢失。
- 确认顺序：**先持久化 ack（fsync）再推进内存位点**。
- 尾部不完整 / CRC 损坏的帧（崩溃写一半）在打开时被截断，只恢复完整记录，
  因此不会出现半提交状态。

### 5.2 优雅停机

`Broker.Close(ctx)`：

1. 关闭调度循环，停止新的投递；
2. 等待在途 handler 完成（受 `ctx` 超时约束）；
3. 已写入的事件/确认均已 fsync，落盘状态完整。

### 5.3 重启恢复（`Open`）

重新打开时回放磁盘：

- **已确认（含死信终止位）的 offset 不会再次投递**；
- **停机时未确认 / 在途 / 退避中的事件，重启后从对应分区水位重新投递**；
- 分区位点、每个消费者组的 `committed`、死信记录与失败原因全部重建；
- 恢复出的消费者组沿用旧位点；重启后新订阅的组从最早留存事件开始消费历史；
- 恢复阶段不做内存驱逐，避免「旧组已追平 → 历史被清空 → 新组读不到历史」。

## 6. 错误区分（内部错误不外泄）

对外通过哨兵错误 + `errors.Is` 区分异常链路，底层系统错误只作为内部 cause，
不会把文件路径等内部细节透出到对外消息：

| 哨兵错误 | 触发场景 |
| --- | --- |
| `ErrInvalidConfig` | 空 DataDir、分区数 ≤ 0、重试倍率 < 1、负的上限等非法配置 |
| `ErrUnknownTopic` | 发布/订阅未声明的主题 |
| `ErrUnknownPart` | 分区不存在 |
| `ErrInvalidArg` | 空 group、nil handler、nil event 等 |
| `ErrOffsetRollback` | 位点回退 |
| `ErrUnknownEvent` | 未知/未投递事件、EventID 不匹配 |
| `ErrDuplicateAck` | 重复确认 |
| `ErrRetryExhausted` | 重试耗尽进入死信 |
| `ErrBackpressure` | 背压拒绝 / 阻塞等待被取消 |
| `ErrBrokerClosed` | Broker 已关闭 |
| `ErrInternal` | 内部错误的统一对外口径（不暴露底层细节） |

## 7. 本地验证方法

要求 Go 1.25+。

```bash
# 构建
go build ./...

# 全部测试（乱序投递、重复投递、重试耗尽、并发确认、背压、停机恢复、WAL 半帧等）
go test -v ./...

# 竞态检测（建议）
go test -race ./...

# 多次运行验证结果确定、可复现
go test -count=5 ./...

# 运行可执行示例
go run ./example
```

测试覆盖（断言结果确定、可复现，日志打印关键位点与判定依据）：

- `TestPartitionOrdering`：队首在途时后续绝不越位，分区内严格按发布顺序处理；
- `TestMultiPartitionParallel` / `TestMultipleGroupsIndependent`：多分区、多组并行；
- `TestSameGroupNoConcurrentDuplicate`：同组同分区在途峰值恒为 1，无并发重复；
- `TestRetryThenSuccess` / `TestNoInfiniteRetry` / `TestRetryExhaustedDLQ`：
  退避重试、重试有界、死信原因与位点推进；
- `TestRedeliveryAcceptedAsDuplicate`：重复投递被正常接受并计入 DuplicateCalls；
- `TestConcurrentAckCommitMonotonic` / `TestConcurrentDuplicateAck`：
  逆序并发确认下 committed 单调递增、并发重复确认恰好 1 次成功；
- `TestDuplicateAndUnknownAckRejected`：重复/未知/篡改确认被明确拒绝；
- `TestBackpressureReject/Block/BlockCancel` / `TestMemoryLimitBackpressure`：
  拒绝/阻塞/取消与内存上限、积压指标；
- `TestRestartRedelivery` / `TestRestartRetryingEvent` /
  `TestRestartDLQPersisted` / `TestRestartNewGroupConsumesHistory`：
  未确认重投、已确认不重投、死信持久化、新组消费历史；
- `TestWALTornTailRecovery`：崩溃半帧被截断，无半提交状态；
- `TestUnknownTopicAndBadConfig` / `TestErrorMessagesDoNotLeakInternals`：
  错误可区分、内部细节不外泄。

## 8. 最小用法

```go
cfg := eventqueue.Config{
    DataDir:       "./data",
    MaxMemorySize: 64 << 20,
    Topics: []eventqueue.TopicConfig{{
        Name:         "orders",
        PartitionNum: 4,
        Backpressure: eventqueue.BackpressureConfig{
            MaxPartitionLag: 10000,
            Mode:            eventqueue.BackpressureBlock,
        },
        Retry: eventqueue.RetryPolicy{
            MaxRetries:  5,
            BaseBackoff: 10 * time.Millisecond,
            MaxBackoff:  time.Second,
            Multiplier:  2,
        },
    }},
}

broker, _ := eventqueue.Open(cfg) // 已存在则恢复
defer broker.Close(context.Background())

idem := eventqueue.NewIdempotentHandler()
broker.RegisterIdempotent(idem)
broker.Subscribe("order-workers", "orders", idem.Wrap(func(ev *eventqueue.Event) error {
    // 处理 ev（同一 Event.ID 的副作用只发生一次）
    return nil
}))

ev, _ := broker.Publish(context.Background(), "orders", "user-123", []byte(`{...}`))
_ = ev
```
