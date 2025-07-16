package lazy

import (
	"context"
	"sync"
)

type counter struct {
	value int32
	mu    sync.Mutex
	cond  *sync.Cond
}

var counters sync.Map

func getOrCreateCounter(key string) *counter {
	actual, _ := counters.LoadOrStore(key, func() any {
		c := &counter{}
		c.cond = sync.NewCond(&c.mu)
		return c
	}())
	return actual.(*counter)
}

// IncrementCounter 增加计数
func IncrementCounter(key string) {
	c := getOrCreateCounter(key)
	c.mu.Lock()
	c.value++
	c.mu.Unlock()
}

// DecrementCounterIfExists 减少计数，并在值为0时广播唤醒等待者
func DecrementCounterIfExists(key string) (int32, bool) {
	val, ok := counters.Load(key)
	if !ok {
		return 0, false
	}
	c := val.(*counter)
	c.mu.Lock()
	c.value--
	newVal := c.value
	if newVal <= 0 {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	return newVal, true
}

// Wait等待直到计数为0
func Wait(ctx context.Context, key string) bool {
	c := getOrCreateCounter(key)

	done := make(chan struct{})

	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for c.value > 0 {
			c.cond.Wait()
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		return false
	case <-done:
		return true
	}
}
