// 本地事件队列最小示例：发布 -> 幂等消费 -> 失败重试 -> 死信 -> 指标。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	eventqueue "github.com/highcumontoa/event-queue-go"
)

func main() {
	dir, err := os.MkdirTemp("", "eq-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := eventqueue.Config{
		DataDir:       dir,
		MaxMemorySize: 4 << 20,
		Topics: []eventqueue.TopicConfig{{
			Name:         "orders",
			PartitionNum: 2,
			Backpressure: eventqueue.BackpressureConfig{
				MaxPartitionLag: 1000,
				Mode:            eventqueue.BackpressureBlock,
			},
			Retry: eventqueue.RetryPolicy{
				MaxRetries:  2,
				BaseBackoff: 5 * time.Millisecond,
				MaxBackoff:  50 * time.Millisecond,
				Multiplier:  2,
			},
		}},
	}
	broker, err := eventqueue.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	}()

	idem := eventqueue.NewIdempotentHandler()
	broker.RegisterIdempotent(idem)

	var mu sync.Mutex
	attempts := map[string]int{}
	h := idem.Wrap(func(ev *eventqueue.Event) error {
		mu.Lock()
		attempts[ev.ID]++
		n := attempts[ev.ID]
		mu.Unlock()
		if string(ev.Payload) == "poison" && n < 3 {
			return errors.New("transient failure")
		}
		fmt.Printf("processed id=%s partition=%d offset=%d payload=%s\n",
			ev.ID, ev.Partition, ev.Offset, ev.Payload)
		return nil
	})
	if _, err := broker.Subscribe("order-workers", "orders", h); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for _, payload := range []string{"a", "b", "poison", "c"} {
		if _, err := broker.Publish(ctx, "orders", "user-1", []byte(payload)); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	st := broker.Stats()
	log.Printf("published=%d delivered=%d acked=%d redelivered=%d dup=%d dead=%d lag=%d",
		st.Published, st.Delivered, st.Acked, st.Redelivered,
		st.DuplicateCalls, st.DeadLettered, st.Lag)
	for _, r := range broker.DLQ() {
		log.Printf("DLQ id=%s offset=%d attempts=%d reason=%q",
			r.EventID, r.Offset, r.Attempts, r.Reason)
	}
}
