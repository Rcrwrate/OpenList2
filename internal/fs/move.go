package fs

import (
	"context"
	"fmt"
	"github.com/OpenListTeam/OpenList/v4/internal/op/lazy"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/OpenListTeam/tache"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type MoveTask CopyTask

func (t *MoveTask) GetName() string {
	return fmt.Sprintf("move [%s](%s) to [%s](%s)", t.SrcStorageMp, t.SrcObjPath, t.DstStorageMp, t.DstDirPath)
}

func (t *MoveTask) GetStatus() string {
	return t.Status
}

func (t *MoveTask) Run() error {
	if err := t.ReinitCtx(); err != nil {
		return err
	}
	t.ClearEndTime()
	t.SetStartTime(time.Now())
	defer func() { t.SetEndTime(time.Now()) }()
	var err error
	if t.srcStorage == nil {
		t.srcStorage, err = op.GetStorageByMountPath(t.SrcStorageMp)
	}
	if t.dstStorage == nil {
		t.dstStorage, err = op.GetStorageByMountPath(t.DstStorageMp)
	}
	if err != nil {
		return errors.WithMessage(err, "failed get storage")
	}
	return moveBetween2Storages(t, t.srcStorage, t.dstStorage, t.SrcObjPath, t.DstDirPath)
}

var MoveTaskManager *tache.Manager[*MoveTask]

func _moveWithValidation(ctx context.Context, srcPath, dstPath string, lazyCache ...bool) (task.TaskExtensionInfo, error) {
	srcStorage, srcObjActualPath, err := op.GetStorageAndActualPath(srcPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get src storage")
	}
	dstStorage, dstDirActualPath, err := op.GetStorageAndActualPath(dstPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get dst storage")
	}

	_, err = op.Get(ctx, srcStorage, srcObjActualPath)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed get src [%s] file", srcPath)
	}

	// Try native move first if in the same storage
	if srcStorage.GetStorage() == dstStorage.GetStorage() {
		err = op.Move(ctx, srcStorage, srcObjActualPath, dstDirActualPath, lazyCache...)
		if !errors.Is(err, errs.NotImplement) && !errors.Is(err, errs.NotSupport) {
			return nil, err
		}
	}

	taskCreator, _ := ctx.Value(conf.UserKey).(*model.User)
	moveTask := &MoveTask{
		TaskExtension: task.TaskExtension{
			Creator: taskCreator,
			ApiUrl:  common.GetApiUrl(ctx),
		},
		srcStorage:   srcStorage,
		dstStorage:   dstStorage,
		SrcObjPath:   srcObjActualPath,
		DstDirPath:   dstDirActualPath,
		SrcStorageMp: srcStorage.GetStorage().MountPath,
		DstStorageMp: dstStorage.GetStorage().MountPath,
	}
	MoveTaskManager.Add(moveTask)
	return moveTask, nil
}

func moveBetween2Storages(t *MoveTask, srcStorage, dstStorage driver.Driver, srcObjPath, dstDirPath string) error {
	t.Status = "getting src object"
	srcObj, err := op.Get(t.Ctx(), srcStorage, srcObjPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] file", srcObjPath)
	}
	if srcObj.IsDir() {
		t.Status = "src object is dir, listing objs"
		objs, err := op.List(t.Ctx(), srcStorage, srcObjPath, model.ListArgs{})
		if err != nil {
			return errors.WithMessagef(err, "failed list src [%s] objs", srcObjPath)
		}
		for _, obj := range objs {
			if utils.IsCanceled(t.Ctx()) {
				return nil
			}
			srcObjPath := stdpath.Join(srcObjPath, obj.GetName())
			dstObjPath := stdpath.Join(dstDirPath, srcObj.GetName())
			moveTask := &MoveTask{

				TaskExtension: task.TaskExtension{
					Creator: t.GetCreator(),
					ApiUrl:  t.ApiUrl,
				},
				srcStorage:   srcStorage,
				dstStorage:   dstStorage,
				SrcObjPath:   srcObjPath,
				DstDirPath:   dstObjPath,
				SrcStorageMp: srcStorage.GetStorage().MountPath,
				DstStorageMp: dstStorage.GetStorage().MountPath,
			}

			MoveTaskManager.Add(moveTask)
		}
		t.Status = "src object is dir, added all move tasks of objs"
		return nil
	}
	return moveFileBetween2Storages(t, srcStorage, dstStorage, srcObjPath, dstDirPath)
}

func moveFileBetween2Storages(tsk *MoveTask, srcStorage, dstStorage driver.Driver, srcFilePath, dstDirPath string) error {
	dstPath := stdpath.Join(dstStorage.GetStorage().MountPath, dstDirPath)
	lazy.IncrementCounter(dstPath)
	defer lazy.DecrementCounterIfExists(dstPath)
	srcFile, err := op.Get(tsk.Ctx(), srcStorage, srcFilePath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] file", srcFilePath)
	}
	tsk.SetTotalBytes(srcFile.GetSize())
	link, _, err := op.Link(tsk.Ctx(), srcStorage, srcFilePath, model.LinkArgs{})
	if err != nil {
		return errors.WithMessagef(err, "failed get [%s] link", srcFilePath)
	}
	// any link provided is seekable
	ss, err := stream.NewSeekableStream(&stream.FileStream{
		Obj: srcFile,
		Ctx: tsk.Ctx(),
	}, link)
	if err != nil {
		_ = link.Close()
		return errors.WithMessagef(err, "failed get [%s] stream", srcFilePath)
	}
	err = op.Put(tsk.Ctx(), dstStorage, dstDirPath, ss, tsk.SetProgress, true)
	if err == nil {
		go func() {
			ctx, cancel := context.WithTimeout(tsk.Ctx(), 1*time.Hour)
			defer cancel()
			if ok := lazy.Wait(ctx, dstPath); !ok {
				log.Warnf("lazy wait timeout or canceled for %s", dstPath)
			}
			_, err = op.Get(tsk.Ctx(), dstStorage, stdpath.Join(dstDirPath, srcFile.GetName()), model.GetArgs{TryRefresh: true})
			if err == nil {
				err = op.Remove(tsk.Ctx(), srcStorage, srcFilePath)
			}
		}()
	}
	return err
}
