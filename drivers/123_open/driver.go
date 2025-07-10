package _123_open

import (
	"context"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type Open123 struct {
	model.Storage
	Addition
}

func (d *Open123) Config() driver.Config {
	return config
}

func (d *Open123) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Open123) Init(ctx context.Context) error {
	if d.UploadThread < 1 || d.UploadThread > 32 {
		d.UploadThread = 3
	}
	//初始化api实例，个人开发者才需要针对账号进行qps限制，第三方应用不需要
	if d.RefreshToken == "" {
		d.apiinstance = map[string]*ApiInfo{
			accessTokenAPI:       InitApiInfo(baseURL+accessTokenAPI, 1),
			refreshTokenAPI:      InitApiInfo(baseURL+refreshTokenAPI, 0),
			userInfoAPI:          InitApiInfo(baseURL+userInfoAPI, 1),
			fileListAPI:          InitApiInfo(baseURL+fileListAPI, 3),
			downloadInfoAPI:      InitApiInfo(baseURL+downloadInfoAPI, 0),
			mkdirAPI:             InitApiInfo(baseURL+mkdirAPI, 2),
			moveAPI:              InitApiInfo(baseURL+moveAPI, 1),
			renameAPI:            InitApiInfo(baseURL+renameAPI, 0),
			trashAPI:             InitApiInfo(baseURL+trashAPI, 0),
			preupCreateAPI:       InitApiInfo(baseURL+preupCreateAPI, 0),
			sliceUploadAPI:       InitApiInfo(sliceUploadAPI, 0),
			uploadCompleteAPI:    InitApiInfo(baseURL+uploadCompleteAPI, 0),
			uploadURLAPI:         InitApiInfo(uploadURLAPI, 0),
			singleUploadAPI:      InitApiInfo(singleUploadAPI, 0),
			preupCreateV1API:     InitApiInfo(baseURL+preupCreateV1API, 0),
			getUploadURLAPI:      InitApiInfo(baseURL+getUploadURLAPI, 0),
			uploadCompleteV1API:  InitApiInfo(baseURL+uploadCompleteV1API, 0),
			uploadAsyncResultAPI: InitApiInfo(baseURL+uploadAsyncResultAPI, 0),
		}
	}
	return nil
}

func (d *Open123) Drop(ctx context.Context) error {
	op.MustSaveDriverStorage(d)
	return nil
}

func (d *Open123) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	fileLastId := int64(0)
	parentFileId, err := strconv.ParseInt(dir.GetID(), 10, 64)
	if err != nil {
		return nil, err
	}
	res := make([]File, 0)

	for fileLastId != -1 {
		files, err := d.getFiles(parentFileId, 100, fileLastId)
		if err != nil {
			return nil, err
		}
		// 目前123panAPI请求，trashed失效，只能通过遍历过滤
		for i := range files.Data.FileList {
			if files.Data.FileList[i].Trashed == 0 {
				res = append(res, files.Data.FileList[i])
			}
		}
		fileLastId = files.Data.LastFileId
	}
	return utils.SliceConvert(res, func(src File) (model.Obj, error) {
		return src, nil
	})
}

func (d *Open123) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	fileId, _ := strconv.ParseInt(file.GetID(), 10, 64)

	res, err := d.getDownloadInfo(fileId)
	if err != nil {
		return nil, err
	}

	link := model.Link{URL: res.Data.DownloadUrl}
	return &link, nil
}

func (d *Open123) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	parentFileId, _ := strconv.ParseInt(parentDir.GetID(), 10, 64)

	return d.mkdir(parentFileId, dirName)
}

func (d *Open123) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	toParentFileID, _ := strconv.ParseInt(dstDir.GetID(), 10, 64)

	return d.move(srcObj.(File).FileId, toParentFileID)
}

func (d *Open123) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	fileId, _ := strconv.ParseInt(srcObj.GetID(), 10, 64)

	return d.rename(fileId, newName)
}

func (d *Open123) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Open123) Remove(ctx context.Context, obj model.Obj) error {
	fileId, _ := strconv.ParseInt(obj.GetID(), 10, 64)

	return d.trash(fileId)
}

func (d *Open123) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) error {
	parentFileId, err := strconv.ParseInt(dstDir.GetID(), 10, 64)
	etag := file.GetHash().GetHash(utils.MD5)

	if len(etag) < utils.MD5.Width {
		_, etag, err = stream.CacheFullInTempFileAndHash(file, utils.MD5)
		if err != nil {
			return err
		}
	}
	createResp, err := d.create(parentFileId, file.GetName(), etag, file.GetSize(), 2, false)
	if err != nil {
		return err
	}
	if createResp.Data.Reuse {
		return nil
	}
	up(10)

	return d.Upload(ctx, file, createResp, up)
}

var _ driver.Driver = (*Open123)(nil)
