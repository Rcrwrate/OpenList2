package aliyundrive_open

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/time/rate"
)

// See document https://www.yuque.com/aliyundrive/zpfszx/mqocg38hlxzc5vcd
// See issue https://github.com/OpenListTeam/OpenList/issues/724
// We got limit per user per app, so the limiter should be global.

type limiter struct {
	list  *rate.Limiter
	link  *rate.Limiter
	other *rate.Limiter
}

var limiters = make(map[string]*limiter)
var limitersLock = &sync.Mutex{}

func getLimiterForUser(userid string) *limiter {
	limitersLock.Lock()
	defer limitersLock.Unlock()
	if lim, ok := limiters[userid]; ok {
		return lim
	}
	lim := &limiter{
		list:  rate.NewLimiter(rate.Limit(3.9), 1),
		link:  rate.NewLimiter(rate.Limit(0.9), 1),
		other: rate.NewLimiter(rate.Limit(14.9), 1),
	}
	limiters[userid] = lim
	return lim
}

type limiterType int

const (
	limiterList limiterType = iota
	limiterLink
	limiterOther
)

func (l *limiter) wait(ctx context.Context, typ limiterType) error {
	if l == nil {
		return fmt.Errorf("driver not init")
	}
	switch typ {
	case limiterList:
		return l.list.Wait(ctx)
	case limiterLink:
		return l.link.Wait(ctx)
	case limiterOther:
		return l.other.Wait(ctx)
	default:
		return fmt.Errorf("unknown limiter type")
	}
}

func (d *AliyundriveOpen) wait(ctx context.Context, typ limiterType) error {
	if d == nil || d.limiter == nil {
		return fmt.Errorf("driver not init")
	}
	return d.limiter.wait(ctx, typ)
}
