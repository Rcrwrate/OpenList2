package batch_task

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type FinishHook func(dstPath string, payloads []any)
type BatchTaskCoordinator struct {
	name string
	mu   sync.Mutex

	pendingPayload map[string][]any
	pendingCount   map[string]int
	allFinishHook  FinishHook
}

func NewBatchTasCoordinator(name string) *BatchTaskCoordinator {
	return &BatchTaskCoordinator{
		name:           name,
		pendingPayload: map[string][]any{},
		pendingCount:   map[string]int{},
	}
}

func (bt *BatchTaskCoordinator) SetAllFinishHook(f FinishHook) *BatchTaskCoordinator {
	bt.allFinishHook = f
	return bt
}

// payload可为nil
func (bt *BatchTaskCoordinator) AddTask(targetPath string, payload any) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	count := bt.pendingCount[targetPath]
	bt.pendingCount[targetPath] = count + 1
	logrus.Debugf("AddTask:%s ,count=%d", targetPath, bt.pendingCount[targetPath])
	if payload == nil {
		return
	}
	if payloads, ok := bt.pendingPayload[targetPath]; ok {
		bt.pendingPayload[targetPath] = append(payloads, payload)
	} else {
		bt.pendingPayload[targetPath] = []any{payload}
	}
}

func (bt *BatchTaskCoordinator) MarkTaskFinish(targetPath string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	count := bt.pendingCount[targetPath]
	if count == 0 {
		return
	}
	logrus.Debugf("MarkTaskFinish:%s ,count=%v", targetPath, count)
	if count == 1 {
		delete(bt.pendingCount, targetPath)
		payloads, ok := bt.pendingPayload[targetPath]
		if ok {
			delete(bt.pendingPayload, targetPath)
		}
		if bt.allFinishHook != nil {
			logrus.Debugf("allFinishHook:%s", targetPath)
			bt.allFinishHook(targetPath, payloads)
		}
		return
	}
	bt.pendingCount[targetPath] = count - 1
}
