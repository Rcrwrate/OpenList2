package batch_task

import (
	"context"
	"path"
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

type MoveSrcPathPayload string

func refreshAndRemove(dstPath string, payloads []any) {
	dstStorage, dstActualPath, err := op.GetStorageAndActualPath(dstPath)
	if err != nil {
		log.Error(errors.WithMessage(err, "failed get dst storage"))
		return
	}
	if _, ok := dstStorage.(driver.PutResult); !ok && !dstStorage.Config().NoCache {
		op.ClearCache(dstStorage, dstActualPath)
	}
	var ctx context.Context
	for _, payload := range payloads {
		if ctx == nil {
			ctx = context.Background()
		}

		if srcPath, srcOk := payload.(MoveSrcPathPayload); srcOk {
			srcStorage, srcActualPath, err := op.GetStorageAndActualPath(string(srcPath))
			if err != nil {
				log.Error(errors.WithMessage(err, "failed get src storage"))
				continue
			}
			if err = verify(ctx, srcStorage, dstStorage, srcActualPath, dstActualPath); err != nil {
				log.Errorf("failed verify: %+v", err)
			} else {
				err = op.Remove(ctx, srcStorage, srcActualPath)
				if err != nil {
					log.Errorf("failed remove %s: %+v", srcPath, err)
				}
			}
		}
	}
}

func verify(ctx context.Context, srcStorage, dstStorage driver.Driver, srcPath, dstPath string) error {
	srcObj, err := op.Get(ctx, srcStorage, srcPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] file", path.Join(srcStorage.GetStorage().MountPath, srcPath))
	}

	dstObjPath := stdpath.Join(dstPath, srcObj.GetName())
	dstObj, err := op.Get(ctx, dstStorage, dstObjPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get dst [%s] file", path.Join(dstStorage.GetStorage().MountPath, dstObjPath))
	}

	if !dstObj.IsDir() {
		return nil
	}

	// Verify directory
	srcObjs, err := op.List(ctx, srcStorage, srcPath, model.ListArgs{})
	if err != nil {
		return errors.WithMessagef(err, "failed list src [%s] objs", path.Join(srcStorage.GetStorage().MountPath, srcPath))
	}

	for _, obj := range srcObjs {
		srcSubPath := stdpath.Join(srcPath, obj.GetName())
		err := verify(ctx, srcStorage, dstStorage, srcSubPath, dstObjPath)
		if err != nil {
			return err
		}
	}
	return nil
}
