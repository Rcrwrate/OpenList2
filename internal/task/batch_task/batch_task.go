package batch_task

import (
	"sync"
)

type BatchTaskHook struct {
	name          string
	mu            sync.Mutex
	tasks         map[string]struct{}
	taskArgs      map[string]map[string]any
	allFinishHook func()
}

func NewBatchTaskHook(name string) *BatchTaskHook {
	return &BatchTaskHook{
		name:     name,
		tasks:    make(map[string]struct{}),
		taskArgs: make(map[string]map[string]any),
	}
}

func (bt *BatchTaskHook) SetAllFinishHook(f func()) *BatchTaskHook {
	bt.allFinishHook = f
	return bt
}

func (bt *BatchTaskHook) AddTask(taskID string, taskMap map[string]any) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.tasks[taskID] = struct{}{}
	if existingMap, ok := bt.taskArgs[taskID]; ok {
		for k, v := range taskMap {
			existingMap[k] = v
		}
	} else {
		newMap := make(map[string]any, len(taskMap))
		for k, v := range taskMap {
			newMap[k] = v
		}
		bt.taskArgs[taskID] = newMap
	}
}

func (bt *BatchTaskHook) RemoveTask(taskID string, allFinish bool) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	delete(bt.tasks, taskID)
	if len(bt.tasks) == 0 && allFinish {
		if bt.allFinishHook != nil {
			bt.allFinishHook()
		}
		bt.taskArgs = make(map[string]map[string]any)
	}
}

func (bt *BatchTaskHook) GetAllTaskArgs() map[string]map[string]any {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	result := make(map[string]map[string]any, len(bt.taskArgs))
	for taskID, args := range bt.taskArgs {
		copyArgs := make(map[string]any, len(args))
		for k, v := range args {
			copyArgs[k] = v
		}
		result[taskID] = copyArgs
	}
	return result
}
