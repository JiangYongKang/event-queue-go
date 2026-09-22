package queue

import "time"

// backoffFor 计算第 attempt 次重试（attempt 从 1 开始，即首次失败后的第一次重投）
// 对应的退避时长：min(InitialBackoff * Multiplier^(attempt-1), MaxBackoff)。
func backoffFor(p RetryPolicy, attempt int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	if p.Multiplier < 1 {
		return p.InitialBackoff
	}
	d := float64(p.InitialBackoff)
	for i := 1; i < attempt; i++ {
		d *= p.Multiplier
		if p.MaxBackoff > 0 && time.Duration(d) >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	out := time.Duration(d)
	if p.MaxBackoff > 0 && out > p.MaxBackoff {
		out = p.MaxBackoff
	}
	return out
}
