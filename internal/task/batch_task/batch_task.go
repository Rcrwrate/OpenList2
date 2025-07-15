package batch_task

import (
	"maps"
	"sync"
)

type BatchTaskHook struct {
	name          string
	mu            sync.Mutex
	tasks         map[string]struct{}
	taskArgs      map[string]TaskMap
	allFinishHook func()
}

func NewBatchTaskHook(name string) *BatchTaskHook {
	return &BatchTaskHook{
		name:     name,
		tasks:    map[string]struct{}{},
		taskArgs: map[string]TaskMap{},
	}
}

func (bt *BatchTaskHook) SetAllFinishHook(f func()) *BatchTaskHook {
	bt.allFinishHook = f
	return bt
}

func (bt *BatchTaskHook) AddTask(taskID string, taskMap TaskMap) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.tasks[taskID] = struct{}{}
	if existingMap, ok := bt.taskArgs[taskID]; ok {
		maps.Copy(existingMap, taskMap)
	} else {
		bt.taskArgs[taskID] = taskMap
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
		clear(bt.taskArgs)
	}
}

func (bt *BatchTaskHook) GetAllTaskArgs() map[string]TaskMap {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	result := map[string]TaskMap{}
	for taskID, args := range bt.taskArgs {
		copyArgs := TaskMap{}
		maps.Copy(copyArgs, args)
		result[taskID] = copyArgs
	}
	return result
}
