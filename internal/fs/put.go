package fs

import (
	"context"
	"fmt"
	"github.com/OpenListTeam/OpenList/v4/internal/task/batch_task"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/tache"
	"github.com/pkg/errors"
)

type UploadTask struct {
	task.TaskExtension
	task.Lifecycle
	storage          driver.Driver
	dstDirActualPath string
	file             model.FileStreamer
}

func (t *UploadTask) GetName() string {
	return fmt.Sprintf("upload %s to [%s](%s)", t.file.GetName(), t.storage.GetStorage().MountPath, t.dstDirActualPath)
}

func (t *UploadTask) GetStatus() string {
	return "uploading"
}

func (t *UploadTask) BeforeRun() error {
	taskMap := make(map[string]any)
	taskMap[batch_task.NeedRefreshPath] = stdpath.Join(t.storage.GetStorage().MountPath, t.dstDirActualPath)
	batch_task.BatchTaskRefreshAndRemoveHook.AddTask(t.GetID(), taskMap)
	return nil
}

func (t *UploadTask) RunCore() error {
	t.ClearEndTime()
	t.SetStartTime(time.Now())
	defer func() { t.SetEndTime(time.Now()) }()
	return op.Put(t.Ctx(), t.storage, t.dstDirActualPath, t.file, t.SetProgress, true)
}

func (t *UploadTask) AfterRun(err error) error {
	allFinish := true
	// 需要先更新任务状态，再进行判断
	if err == nil {
		t.State = tache.StateSucceeded
	} else {
		t.State = tache.StateFailed
	}
	for _, ct := range UploadTaskManager.GetAll() {
		if !utils.SliceContains([]tache.State{
			tache.StateSucceeded,
			tache.StateFailed,
			tache.StateCanceled,
		}, ct.GetState()) {
			allFinish = false
		}
	}
	batch_task.BatchTaskRefreshAndRemoveHook.RemoveTask(t.GetID(), allFinish)
	return err
}

func (t *UploadTask) Run() error {
	return task.RunWithLifecycle(t)
}

var UploadTaskManager *tache.Manager[*UploadTask]

// putAsTask add as a put task and return immediately
func putAsTask(ctx context.Context, dstDirPath string, file model.FileStreamer) (task.TaskExtensionInfo, error) {
	storage, dstDirActualPath, err := op.GetStorageAndActualPath(dstDirPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get storage")
	}
	if storage.Config().NoUpload {
		return nil, errors.WithStack(errs.UploadNotSupported)
	}
	if file.NeedStore() {
		_, err := file.CacheFullInTempFile()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create temp file")
		}
		//file.SetReader(tempFile)
		//file.SetTmpFile(tempFile)
	}
	taskCreator, _ := ctx.Value(conf.UserKey).(*model.User) // taskCreator is nil when convert failed
	t := &UploadTask{
		TaskExtension: task.TaskExtension{
			Creator: taskCreator,
		},
		storage:          storage,
		dstDirActualPath: dstDirActualPath,
		file:             file,
	}
	t.SetTotalBytes(file.GetSize())
	UploadTaskManager.Add(t)
	return t, nil
}

// putDirect put the file and return after finish
func putDirectly(ctx context.Context, dstDirPath string, file model.FileStreamer, lazyCache ...bool) error {
	storage, dstDirActualPath, err := op.GetStorageAndActualPath(dstDirPath)
	if err != nil {
		_ = file.Close()
		return errors.WithMessage(err, "failed get storage")
	}
	if storage.Config().NoUpload {
		_ = file.Close()
		return errors.WithStack(errs.UploadNotSupported)
	}
	return op.Put(ctx, storage, dstDirActualPath, file, nil, lazyCache...)
}
