package eventqueue

import "errors"

// 对外暴露的可区分错误哨兵。内部错误不得直接透出，统一用 queueError 包装。
var (
	ErrInvalidConfig  = errors.New("invalid configuration")
	ErrUnknownTopic   = errors.New("unknown topic")
	ErrUnknownPart    = errors.New("unknown partition")
	ErrInvalidArg     = errors.New("invalid argument")
	ErrOffsetRollback = errors.New("offset rollback rejected")
	ErrUnknownEvent   = errors.New("unknown or undelivered event")
	ErrDuplicateAck   = errors.New("duplicate acknowledgement rejected")
	ErrRetryExhausted = errors.New("retry exhausted, event moved to dead letter")
	ErrBackpressure   = errors.New("backpressure: publish rejected")
	ErrBrokerClosed   = errors.New("broker is closed")
	ErrInternal       = errors.New("internal error")
)

// queueError 统一错误类型：Kind 为上面的哨兵错误，便于 errors.Is 区分；
// Err 为内部原因，仅写入日志，不直接暴露对外消息。
type queueError struct {
	Kind error
	Op   string
	Msg  string
	Err  error
}

func (e *queueError) Error() string {
	var s string
	if e.Kind != nil {
		s = e.Kind.Error()
	} else {
		s = ErrInternal.Error()
	}
	if e.Op != "" {
		s += ": " + e.Op
	}
	if e.Msg != "" {
		s += ": " + e.Msg
	}
	return s
}

func (e *queueError) Unwrap() []error {
	if e.Kind == nil {
		if e.Err == nil {
			return []error{ErrInternal}
		}
		return []error{ErrInternal, e.Err}
	}
	if e.Err == nil {
		return []error{e.Kind}
	}
	return []error{e.Kind, e.Err}
}

func newError(kind error, op, msg string, cause error) error {
	return &queueError{Kind: kind, Op: op, Msg: msg, Err: cause}
}
