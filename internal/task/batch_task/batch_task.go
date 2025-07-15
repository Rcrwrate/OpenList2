package batch_task

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type taskKey int

const (
	_ taskKey = iota
	refreshPath
	MoveSrcPath
	MoveDstPath
)

type TaskPayload map[taskKey]any
type FinishHook func(payloads []TaskPayload)
type BatchTaskCoordinator struct {
	name string
	mu   sync.Mutex

	pendingTasks  map[string][]TaskPayload
	finishCounts  map[string]int
	allFinishHook FinishHook
}

func NewBatchTasCoordinator(name string) *BatchTaskCoordinator {
	return &BatchTaskCoordinator{
		name:         name,
		pendingTasks: map[string][]TaskPayload{},
		finishCounts: map[string]int{},
	}
}

func (bt *BatchTaskCoordinator) SetAllFinishHook(f FinishHook) *BatchTaskCoordinator {
	bt.allFinishHook = f
	return bt
}

// 自动添加refreshPath在第1个任务
func (bt *BatchTaskCoordinator) AddTask(targetPath string, payload TaskPayload) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	defer func() {
		logrus.Debugf("AddTask:%s ,%d", targetPath, len(bt.pendingTasks[targetPath]))
	}()
	if payloads, ok := bt.pendingTasks[targetPath]; ok {
		if _, ok := payload[refreshPath]; ok {
			if len(payload) == 1 {
				t := payloads[0]
				t[refreshPath] = targetPath
				payloads[0] = t
				return
			}
			delete(payload, refreshPath)
		}
		bt.pendingTasks[targetPath] = append(payloads, payload)
	} else {
		payload[refreshPath] = targetPath
		bt.pendingTasks[targetPath] = []TaskPayload{payload}
	}
}

func (bt *BatchTaskCoordinator) MarkTaskFinish(targetPath string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	finishCount := bt.finishCounts[targetPath]
	finishCount++
	logrus.Debugf("MarkTaskFinish:%s ,%v", targetPath, finishCount)
	if payloads, ok := bt.pendingTasks[targetPath]; ok {
		if len(payloads) == finishCount {
			delete(bt.pendingTasks, targetPath)
			delete(bt.finishCounts, targetPath)
			if bt.allFinishHook != nil {
				logrus.Debugf("allFinishHook:%s", targetPath)
				bt.allFinishHook(payloads)
			}
			return
		}
	}
	bt.finishCounts[targetPath] = finishCount
}
