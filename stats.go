package eventqueue

import "sync"

// dupRegistry 汇总全部已注册 IdempotentHandler 的重复投递计数，
// 使 Broker.Stats 能输出 DuplicateCalls 指标。
type dupRegistry struct {
	mu       sync.Mutex
	handlers []*IdempotentHandler
}

func (r *dupRegistry) register(h *IdempotentHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, h)
}

func (r *dupRegistry) total() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, h := range r.handlers {
		n += h.DuplicateCalls()
	}
	return n
}
