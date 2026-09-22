package queue

import "sync"

// DedupStore 为消费者侧幂等去重存储（并发安全）。
// 投递语义为至少一次，事件可能被重复投递；消费者在执行业务副作用前调用 Claim，
// 副作用成功后即持有令牌；若处理失败需调用 Release 释放，使重试仍可执行副作用。
// 重复投递被视为正常情况，由统计中的 Duplicates 计数体现，而不是按失败处理。
type DedupStore struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func NewDedupStore() *DedupStore {
	return &DedupStore{m: make(map[string]struct{})}
}

// Claim 尝试占用 eventID：首次返回 true；已占用（重复）返回 false。
func (d *DedupStore) Claim(eventID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.m[eventID]; ok {
		return false
	}
	d.m[eventID] = struct{}{}
	return true
}

// Release 释放令牌（业务处理失败时调用），保证后续重试能重新执行副作用。
func (d *DedupStore) Release(eventID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.m, eventID)
}

// Check 报告 eventID 是否已见过且不改变状态（供查询/测试）。
func (d *DedupStore) Check(eventID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.m[eventID]
	return ok
}

// Len 返回已持有的去重令牌数量。
func (d *DedupStore) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.m)
}
