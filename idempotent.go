package eventqueue

import (
	"sync"
	"sync/atomic"
)

// IdempotentHandler 用事件 ID 对已成功处理的事件做幂等去重。
//
// 语义：在至少一次投递下，同一个事件可能被多次投递。包装器保证业务 handler
// 对同一 EventID 只成功执行一次；后续重复投递直接返回 nil（视为成功），
// 并计入 DuplicateCalls —— 重复投递是正常情况，绝不能当作失败重试。
type IdempotentHandler struct {
	seen sync.Map // map[string]struct{}
	dup  atomic.Int64
}

func NewIdempotentHandler() *IdempotentHandler {
	return &IdempotentHandler{}
}

// Wrap 返回幂等包装后的 Handler。
// 仅当业务 handler 返回 nil 时才把事件 ID 标记为已处理：
// 业务失败时不记录，以便重试时业务逻辑仍会被执行。
func (h *IdempotentHandler) Wrap(fn Handler) Handler {
	return func(ev *Event) error {
		if _, loaded := h.seen.LoadOrStore(ev.ID, struct{}{}); loaded {
			h.dup.Add(1)
			return nil
		}
		if err := fn(ev); err != nil {
			// 业务失败：删除标记，允许重试再次执行
			h.seen.Delete(ev.ID)
			return err
		}
		return nil
	}
}

// DuplicateCalls 返回被识别并正常接受的重复投递次数。
func (h *IdempotentHandler) DuplicateCalls() int64 {
	return h.dup.Load()
}
