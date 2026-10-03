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

## 消费端晚到 / 不在场（先发布、后订阅）

生产端可以先启动、先写事件，消费组很久之后才订阅；也允许某些分区长时间没有
任何消费组。这是一等支持的节奏，语义与“消费者在场”时完全一致：

- **历史不丢、不乱序**：任何已成功 `Publish`（已 fsync）的事件都先落在
  `events.log` 里。消费组首次订阅时从该分区保留窗口的最早位点（base offset）
  开始，由“分区 × 组”派发器按 offset 顺序逐条投递历史。即使这些历史事件的
  **事件体当前并不在内存里**（长跑淘汰或重启后），派发器也会在需要时按块
  （64 条/次）从日志**缺页读回**再投递；投递之后的顺序、连续前缀位点推进、
  重试退避、死信与幂等去重（事件 ID 仍绑定 `(topic,partition,offset)`）
  与实时事件没有任何区别。
- **重启后同样成立**：恢复时只重建轻量元数据（位点 + 每条 payload 长度），
  不把事件体载入内存；无订阅者的历史积压重启后依旧可从头读盘投递。
- **积压口径不依赖订阅者**：没有任何消费组时，未确认积压按**整个保留窗口**
  计算（最慢消费组的 committed offset 视为 base offset）。因此
  `MaxBacklogPerPartition` / `MaxMemoryBytes` 在无人订阅、或所有组都已追平时
  照样生效：Reject 立即返回 `ErrBackpressure` 并计 `Rejected`，Block 照常阻塞，
  `Broker.Backlog()` 的 `Pending / InFlight / PayloadBytes` 如实反映容量占用。

## 容量与内存语义（常驻内存只随上限，不随累计发布量）

- 分区在内存中维护一个滑动驻留窗口 `[base, head)`：
  - `head` = 日志总行数（下一条事件位点），`base` = 仍需保留的最早位点；
  - `sizes` 是窗口内每条 payload 长度的元数据环（每事件约一个 int），
    因此即便事件体不在内存也能精确统计积压字节；
  - `events` 只稀疏驻留事件体，缺页（历史事件、重启恢复）时从日志按块读回。
- 当某段前缀被**所有**消费组连续确认（或死信跳过）且位点已持久化后，
  该前缀的事件体与元数据被淘汰出内存，`base` 随之推进。这是纯粹的容量控制：
  - 不删除磁盘日志，不影响重启恢复（恢复只依赖 `events.log` + `offsets.json`）；
  - 不会把已确认事件重新投给既有消费组；
  - 不会越过仍在消费的组——淘汰下界取所有组的最小 committed（含未实例化的
    恢复组），慢组不追平，前缀就不会被淘汰；
  - 不打乱任何组的位点与顺序。
- 结论：累计发布量可以远大于上限，常驻内存只与配置的积压/字节上限相关
  （加上每事件一条等长元数据）；磁盘日志承担完整历史。
- 新消费组在已有历史被确认淘汰之后才订阅时，从当前 base 开始消费（不会、也
  无法重放已被所有组确认并淘汰的更早历史）；在淘汰发生之前订阅则从最早
  历史开始。

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

以分区内**最慢消费组**的 committed offset 计算未确认积压；**没有任何消费组
订阅时按整个保留窗口计算**，容量判断不会因订阅者缺席而失效：

- `MaxBacklogPerPartition`：未确认事件数上限。
- `MaxMemoryBytes`：未确认事件 payload 字节上限（只依赖元数据统计，事件体
  是否驻留内存都准确）。
- 策略：
  - `BackpressureReject`：超限立即返回 `ErrBackpressure`，并计入 `Stats.Rejected`；
  - `BackpressureBlock`：`Publish` 阻塞，直到位点推进腾出空间或 `ctx` 取消
    （返回 ctx 错误，不会死锁）；消费组晚到并确认后，被阻塞的发布会自动放行。
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
- `TestPublishBeforeSubscribe`：先发布 100 条、消费组晚到后从头按序消费全部
  历史；无订阅者期间积压/字节指标如实。
- `TestHistoryDeliveredFromDiskAfterRestart`：无订阅者发布 70 条（>缺页块 64）
  后重启，恢复时事件体 0 驻留，订阅后经缺页读盘按序投递、位点正常推进。
- `TestBackpressureWithoutConsumers` / `TestBlockWithoutConsumersReleasedOnLateSubscribe`：
  完全无消费者时发到接近/超过条数上限与字节上限，拒绝/阻塞行为、`Rejected`
  计数与指标符合预期；晚到确认后放行。
- `TestMemoryBoundedOverLongRun`：累计发布 300（远超上限 8），驻留内存始终
  有界、追平后归零；重启后晚到组不重投已确认历史、只收新事件。
- `TestRestartRecovery`：未确认事件重启后按序重投、已确认不重投；
  `TestRestartNoRedeliveryAfterAck`、`TestRestartDLQPersistence`：全确认与
  死信场景的恢复正确性。
