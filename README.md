# event-queue-go

本地事件队列与异步投递系统。用本地文件存储模拟消息落盘与恢复，提供主题 / 分区 /
消费组的发布订阅模型，支持并行消费、失败重试退避、死信、背压、优雅停机与重启恢复。

## 模型与投递语义

- **主题 / 分区**：事件按 Key 的 FNV-1a 哈希落到分区；分区内 offset 从 0 开始严格
  按发布顺序递增，事件 ID 确定性地绑定 `(topic, partition, offset)`，全局唯一且
  重启后不变。
- **消费组**：同组多个消费者并行消费；每个“分区 × 消费组”有一个有序派发协程，
  事件严格按 offset 顺序进入组内共享通道。单分区在途窗口由
  `MaxInFlightPerPartition` 控制（默认 1，即前一事件确认/死信后才派发下一个）。
- **至少一次（at-least-once）**：事件可能被重复投递（重试、或“已处理未确认”时
  停机恢复）。消费者必须幂等：使用 `DedupStore.Claim/Release` 或
  `Broker.NewDedupHandler` 在副作用前去重；重复投递被正常接受并计入
  `Stats.Duplicates`，不会被当作失败。
- **顺序保证**：分区内事件按发布顺序派发；确认允许乱序到达，但已确认位点
  （committed offset）只按**连续前缀**推进，任何情况下不回退、不越位。
  消费者侧需保证副作用与确认的配合（建议先落地副作用再 Ack）。

## 确认规则（并发安全）

- `Ack(e)` 成功后该位点计入连续前缀，位点文件原子持久化（临时文件 + rename）。
- 对已确认位点再次确认：`ErrDuplicateAck`（明确拒绝，不是成功幂等）。
- 对未投递 / 不存在的事件确认：`ErrUnknownEvent`。
- 用旧投递版本号（重试前的副本）确认：`ErrOffsetRegression`。
- 在途记录带投递版本号 `Attempts`，陈旧 Nack 同样返回 `ErrOffsetRegression`。

## 重试与死信规则

- `RetryPolicy{MaxAttempts, InitialBackoff, MaxBackoff, Multiplier}`。
- 处理失败（`Nack`）后按指数退避重投：
  `backoff = min(InitialBackoff * Multiplier^(attempt-1), MaxBackoff)`。
- 投递次数达到 `MaxAttempts`（含首次）仍失败：事件进入死信队列（DLQ），
  记录包含事件本体、消费组、尝试次数、失败时间与**可查询的失败原因**
  （`retries exhausted after N attempts: <cause>`）。
- 死信为终态：位点推进越过该事件，分区不会被卡死，且重启后死信不重投。
  不会无限重试，也不会静默丢弃。可通过 `Broker.DeadLetters(topic, group)` 查询。

## 持久化与恢复（无半提交）

数据目录布局：

```
<DataDir>/
  <topic>/<partition>/events.log     # 事件 JSON 行，追加 + fsync
  <topic>/<partition>/offsets.json   # {消费组: committed offset}，原子替换
  dlq.log                            # 死信 JSON 行，追加
```

- 发布：**先落盘后入内存**，落盘失败的事件绝不会被派发。
- 确认：先原子写位点文件，成功后才推进内存簿记；不存在内存已前进而磁盘未提交。
- 优雅停机（`Close`）：停止全部派发协程；已确认事件不再投递，**未确认事件
  （含处理中）重启后从已提交位点重新投递**。
- 重启：扫描目录恢复主题、事件与各消费组位点；DLQ 记录一并恢复。

## 背压策略

以分区内**最慢消费组**的 committed offset 计算未确认积压：

- `MaxBacklogPerPartition`：未确认事件数上限。
- `MaxMemoryBytes`：未确认事件 payload 字节上限。
- 策略：
  - `BackpressureReject`：超限立即返回 `ErrBackpressure`，并计入 `Stats.Rejected`；
  - `BackpressureBlock`：`Publish` 阻塞，直到位点推进腾出空间或 `ctx` 取消
    （返回 ctx 错误，不会死锁）。
- 指标：`Broker.Backlog()` 返回每分区 `Pending / InFlight / PayloadBytes`。
  系统不会无界占用内存，也不会悄悄丢弃事件。

## 可区分的错误

| 场景 | 错误 |
| --- | --- |
| 非法配置（空目录、分区数/重试次数非法等） | `ErrInvalidConfig` |
| 未知主题 | `ErrUnknownTopic` |
| 未知分区 | `ErrUnknownPartition` |
| 位点回退（旧投递版本确认） | `ErrOffsetRegression` |
| 确认未投递 / 不存在的事件 | `ErrUnknownEvent` |
| 重复确认 | `ErrDuplicateAck` |
| 重试耗尽（对外以 DLQ + 原因暴露） | 死信 `Reason` |
| 背压拒绝 | `ErrBackpressure` |
| Broker 已关闭 | `ErrClosed` |
| 本地落盘/恢复失败（内部错误不透出） | `ErrPersistence` |

内部 I/O 错误统一包装为上述对外错误，不直接透出内部错误类型；用 `errors.Is` 判断。

## 快速上手

```go
b, err := queue.NewBroker(queue.Config{
    DataDir: "./data", NumPartitions: 4,
    MaxBacklogPerPartition: 1000,
    BackpressurePolicy:     queue.BackpressureBlock,
    MaxInFlightPerPartition: 4,
    Retry: queue.RetryPolicy{MaxAttempts: 5, InitialBackoff: 10*time.Millisecond,
        MaxBackoff: time.Second, Multiplier: 2},
})
b.CreateTopic("orders", 4)

c, _ := b.Subscribe("orders", "billing", "worker-1")
dedup := queue.NewDedupStore()
h := b.NewDedupHandler(dedup, func(ctx context.Context, e queue.Event) error {
    // 执行业务副作用（只会执行一次）
    return nil
})
for {
    ev, err := c.Poll(ctx)
    if err != nil { break }
    if err := h(ctx, ev); err != nil { _ = c.Nack(ev, err) } else { _ = c.Ack(ev) }
}
```

## 本地验证

```bash
go vet ./...
go test -race -count=1 ./...        # 全量测试 + 竞态检测
go test -v ./...                     # 查看关键位点与判定依据日志
```

测试覆盖（结果确定、可复现，均使用 `t.TempDir()` 隔离）：

- `TestInOrderDelivery`：3 消费者并行，断言严格按序投递/确认、无丢失。
- `TestConcurrentOutOfOrderAck`：8 消费者 + 在途窗口 4，随机耗时导致乱序确认，
  断言位点单调递增、最终精确到位、每个事件副作用恰好一次（`-race`）。
- `TestAckErrors` / `TestStaleAndDuplicateAckUnderRetry`：未知事件、重复确认、
  位点回退的错误区分。
- `TestRetryThenAck`：退避重试后成功；`TestRetriesExhaustedToDLQ`：耗尽后死信
  携因、位点推进、分区不卡死。
- `TestDuplicateDeliveryAccepted` / `TestRestartDuplicateAcceptedWithDedup`：
  重复投递幂等接受、计 Duplicates、不产生重复副作用。
- `TestBackpressureReject` / `TestBackpressureBlock`：拒绝/阻塞策略与积压指标。
- `TestRestartRecovery`：未确认事件重启后按序重投、已确认不重投；
  `TestRestartNoRedeliveryAfterAck`、`TestRestartDLQPersistence`：全确认与
  死信场景的恢复正确性。
