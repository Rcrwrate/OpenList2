package batch_task

import (
	"sync"
)

type BatchTaskHook struct {
	name          string
	mu            sync.Mutex
	Tasks         map[string]struct{}
	TaskArgs      map[string]map[string]any
	AllFinishHook func()
}

func NewBatchTaskHook(name string) *BatchTaskHook {
	return &BatchTaskHook{
		name:     name,
		Tasks:    make(map[string]struct{}),
		TaskArgs: make(map[string]map[string]any),
	}
}

func (bt *BatchTaskHook) SetAllFinishHook(f func()) *BatchTaskHook {
	bt.AllFinishHook = f
	return bt
}

func (bt *BatchTaskHook) AddTask(taskID string, taskMap map[string]any) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.Tasks[taskID] = struct{}{}
	if existingMap, ok := bt.TaskArgs[taskID]; ok {
		for k, v := range taskMap {
			existingMap[k] = v
		}
	} else {
		bt.TaskArgs[taskID] = taskMap
	}
}

func (bt *BatchTaskHook) RemoveTask(taskID string, allFinish bool) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	delete(bt.Tasks, taskID)
	if len(bt.Tasks) == 0 && allFinish {
		bt.AllFinishHook()
		bt.TaskArgs = make(map[string]map[string]any)
	}
}
