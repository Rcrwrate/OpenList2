package batch_task

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var BatchTaskRefreshAndRemoveHook *BatchTaskCoordinator

func InitBatchTaskHook() {
	BatchTaskRefreshAndRemoveHook = NewBatchTasCoordinator("refreshAndRemoveHook")
	BatchTaskRefreshAndRemoveHook.SetAllFinishHook(refreshAndRemove)
}

func refreshAndRemove(payloads []TaskPayload) {
	for _, payload := range payloads {
		if refreshPathRaw, ok := payload[refreshPath]; ok {
			if refreshPath, ok := refreshPathRaw.(string); ok {
				storage, actualPath, _ := op.GetStorageAndActualPath(refreshPath)
				op.ClearCache(storage, actualPath)
			}
		}
		if srcPathRaw, ok := payload[MoveSrcPath]; ok {
			if dstPathRaw, ok := payload[MoveDstPath]; ok {
				srcPath, srcOk := srcPathRaw.(string)
				dstPath, dstOk := dstPathRaw.(string)
				if srcOk && dstOk {
					srcStorage, srcActualPath, err := op.GetStorageAndActualPath(srcPath)
					if err != nil {
						log.Errorf("Failed to get storage for path: %v", err)
						continue
					}
					dstStorage, dstActualPath, err := op.GetStorageAndActualPath(dstPath)
					if err != nil {
						log.Errorf("Failed to get storage for path: %v", err)
						continue
					}
					_ = verifyAndRemove(srcStorage, dstStorage, srcActualPath, dstActualPath)
				}
			}
		}
	}
}

func verifyAndRemove(srcStorage, dstStorage driver.Driver, srcPath, dstPath string) error {
	ctx := context.Background()
	srcObj, err := op.Get(ctx, srcStorage, srcPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] object", srcPath)
	}

	if !srcObj.IsDir() {
		// Verify single file
		dstFilePath := stdpath.Join(dstPath, srcObj.GetName())
		_, err := op.Get(ctx, dstStorage, dstFilePath)
		if err != nil {
			return errors.WithMessagef(err, "verification failed: destination file [%s] not found", dstFilePath)
		}
		return op.Remove(ctx, srcStorage, srcPath)
	}

	// Verify directory
	dstObjPath := stdpath.Join(dstPath, srcObj.GetName())
	_, err = op.Get(ctx, dstStorage, dstObjPath)
	if err != nil {
		return errors.WithMessagef(err, "verification failed: destination directory [%s] not found", dstObjPath)
	}

	// Verify directory contents
	srcObjs, err := op.List(ctx, srcStorage, srcPath, model.ListArgs{})
	if err != nil {
		return errors.WithMessagef(err, "failed list src [%s] objs for verification", srcPath)
	}

	hasError := false
	for _, obj := range srcObjs {
		srcSubPath := stdpath.Join(srcPath, obj.GetName())
		err := verifyAndRemove(srcStorage, dstStorage, srcSubPath, dstObjPath)
		if err != nil {
			hasError = true
		}
	}
	// 如果目录下文件全部删除成功,则删除目录本身
	if hasError {
		return errors.Errorf("some subitems of [%s] failed to verify and remove", srcPath)
	}
	_ = op.Remove(ctx, srcStorage, srcPath)
	return nil
}
