package fs

import (
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/tache"
)

func WaitALL(tasks []task.TaskExtensionInfo) {
	WaitALLAndDo(tasks, nil)
}

func WaitALLAndDo(tasks []task.TaskExtensionInfo, fn func()) {
	if len(tasks) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for _, t := range tasks {
		go func(t task.TaskExtensionInfo) {
			defer wg.Done()
			for {
				state := t.GetState()
				if utils.SliceContains([]tache.State{
					tache.StateSucceeded, tache.StateFailed,
					tache.StateCanceled, tache.StateErrored,
				}, state) {
					return
				}
				time.Sleep(300 * time.Millisecond)
			}
		}(t)
	}
	wg.Wait()
	if fn != nil {
		fn()
	}
}
