package fs

import (
	"context"
	"fmt"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/internal/task/batch_task"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/pkg/errors"
)

func _moveWithValidation(ctx context.Context, srcPath, dstPath string, validateExistence bool, lazyCache ...bool) error {
	srcStorage, srcObjActualPath, err := op.GetStorageAndActualPath(srcPath)
	if err != nil {
		return errors.WithMessage(err, "failed get src storage")
	}
	dstStorage, dstDirActualPath, err := op.GetStorageAndActualPath(dstPath)
	if err != nil {
		return errors.WithMessage(err, "failed get dst storage")
	}

	_, err = op.Get(ctx, srcStorage, srcObjActualPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] object", srcPath)
	}

	// Try native move first if in the same storage
	if srcStorage.GetStorage() == dstStorage.GetStorage() {
		err = op.Move(ctx, srcStorage, srcObjActualPath, dstDirActualPath, lazyCache...)
		if !errors.Is(err, errs.NotImplement) && !errors.Is(err, errs.NotSupport) {
			return err
		}
	}

	taskCreator, _ := ctx.Value(conf.UserKey).(*model.User)
	copyTask := &CopyTask{
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

	taskID := fmt.Sprintf("%p", copyTask)
	copyTask.SetID(taskID)
	batch_task.BatchTaskRefreshAndRemoveHook.AddTask(taskID, batch_task.TaskMap{
		batch_task.MoveSrcPath: stdpath.Join(copyTask.SrcStorageMp, srcObjActualPath),
		batch_task.MoveDstPath: stdpath.Join(copyTask.DstStorageMp, dstDirActualPath),
	})
	CopyTaskManager.Add(copyTask)
	return nil
}
