package _123

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/OpenListTeam/OpenList/drivers/base"
	"github.com/OpenListTeam/OpenList/internal/driver"
	"github.com/OpenListTeam/OpenList/internal/errs"
	"github.com/OpenListTeam/OpenList/internal/model"
	"github.com/OpenListTeam/OpenList/internal/stream"
	"github.com/OpenListTeam/OpenList/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

type Pan123 struct {
	model.Storage
	Addition
	apiRateLimit sync.Map
}

func (d *Pan123) Config() driver.Config {
	return config
}

func (d *Pan123) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan123) Init(ctx context.Context) error {
	_, err := d.Request(UserInfo, http.MethodGet, nil, nil)
	return err
}

func (d *Pan123) Drop(ctx context.Context) error {
	_, _ = d.Request(Logout, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{})
	}, nil)
	return nil
}

func (d *Pan123) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	files, err := d.getFiles(ctx, dir.GetID(), dir.GetName())
	if err != nil {
		return nil, err
	}
	return utils.SliceConvert(files, func(src File) (model.Obj, error) {
		return src, nil
	})
}

func (d *Pan123) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if f, ok := file.(File); ok {
		var headers map[string]string
		if !utils.IsLocalIPAddr(args.IP) {
			headers = map[string]string{
				"X-Forwarded-For": args.IP,
			}
		}

		data := base.Json{
			"driveId":   0,
			"etag":      f.Etag,
			"fileId":    f.FileId,
			"fileName":  f.FileName,
			"s3keyFlag": f.S3KeyFlag,
			"size":      f.Size,
			"type":      f.Type,
		}

		resp, err := d.Request(DownloadInfo, http.MethodPost, func(req *resty.Request) {
			req.SetBody(data).SetHeaders(headers)
		}, nil)
		if err != nil {
			return nil, err
		}

		downloadUrl := utils.Json.Get(resp, "data", "DownloadUrl").ToString()
		u, err := url.Parse(downloadUrl)
		if err != nil {
			return nil, err
		}

		origin := u.Scheme + "://" + u.Host
		params := u.Query().Get("params")

		if strings.Contains(origin, "web-pro") && params != "" {
			decoded, err := base64.StdEncoding.DecodeString(params)
			if err != nil {
				return nil, err
			}
			directURL, err := url.Parse(string(decoded))
			if err != nil {
				return nil, err
			}
			directURL.Query().Set("auto_redirect", "0")
			u.Query().Set("params", base64.StdEncoding.EncodeToString([]byte(directURL.String())))
			downloadUrl = u.String()
		} else {
			// 非 web-pro 的 origin：将整个原始 URL 打包进 web-pro2 地址中
			u.Query().Set("auto_redirect", "0")
			repack := &url.URL{
				Scheme: "https",
				Host:   "web-pro2.123952.com",
				Path:   "/download-v2/",
			}
			q := repack.Query()
			q.Set("params", base64.StdEncoding.EncodeToString([]byte(url.QueryEscape(u.String()))))
			q.Set("is_s3", "0")
			repack.RawQuery = q.Encode()
			downloadUrl, _ = url.QueryUnescape(repack.String())
		}

		log.Debug("final download url (after patch): ", downloadUrl)
		res, err := base.NoRedirectClient.R().SetHeader("Referer", "https://www.123pan.com/").Get(downloadUrl)
		if err != nil {
			return nil, err
		}

		link := model.Link{
			URL: downloadUrl,
			Header: http.Header{
				"Referer": []string{"https://www.123pan.com/"},
			},
		}

		if res.StatusCode() == 302 {
			link.URL = res.Header().Get("location")
		} else if res.StatusCode() < 300 {
			link.URL = utils.Json.Get(res.Body(), "data", "redirect_url").ToString()
		}

		return &link, nil
	}

	return nil, fmt.Errorf("can't convert obj")
}

func (d *Pan123) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	data := base.Json{
		"driveId":      0,
		"etag":         "",
		"fileName":     dirName,
		"parentFileId": parentDir.GetID(),
		"size":         0,
		"type":         1,
	}
	_, err := d.Request(Mkdir, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *Pan123) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	data := base.Json{
		"fileIdList":   []base.Json{{"FileId": srcObj.GetID()}},
		"parentFileId": dstDir.GetID(),
	}
	_, err := d.Request(Move, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *Pan123) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	data := base.Json{
		"driveId":  0,
		"fileId":   srcObj.GetID(),
		"fileName": newName,
	}
	_, err := d.Request(Rename, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *Pan123) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan123) Remove(ctx context.Context, obj model.Obj) error {
	if f, ok := obj.(File); ok {
		data := base.Json{
			"driveId":           0,
			"operation":         true,
			"fileTrashInfoList": []File{f},
		}
		_, err := d.Request(Trash, http.MethodPost, func(req *resty.Request) {
			req.SetBody(data)
		}, nil)
		return err
	} else {
		return fmt.Errorf("can't convert obj")
	}
}

func (d *Pan123) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) error {
	etag := file.GetHash().GetHash(utils.MD5)
	var err error
	if len(etag) < utils.MD5.Width {
		_, etag, err = stream.CacheFullInTempFileAndHash(file, utils.MD5)
		if err != nil {
			return err
		}
	}
	data := base.Json{
		"driveId":      0,
		"duplicate":    2, // 2->覆盖 1->重命名 0->默认
		"etag":         etag,
		"fileName":     file.GetName(),
		"parentFileId": dstDir.GetID(),
		"size":         file.GetSize(),
		"type":         0,
	}
	var resp UploadResp
	res, err := d.Request(UploadRequest, http.MethodPost, func(req *resty.Request) {
		req.SetBody(data).SetContext(ctx)
	}, &resp)
	if err != nil {
		return err
	}
	log.Debugln("upload request res: ", string(res))
	if resp.Data.Reuse || resp.Data.Key == "" {
		return nil
	}
	if resp.Data.AccessKeyId == "" || resp.Data.SecretAccessKey == "" || resp.Data.SessionToken == "" {
		err = d.newUpload(ctx, &resp, file, up)
		return err
	} else {
		cfg := &aws.Config{
			Credentials:      credentials.NewStaticCredentials(resp.Data.AccessKeyId, resp.Data.SecretAccessKey, resp.Data.SessionToken),
			Region:           aws.String("123pan"),
			Endpoint:         aws.String(resp.Data.EndPoint),
			S3ForcePathStyle: aws.Bool(true),
		}
		s, err := session.NewSession(cfg)
		if err != nil {
			return err
		}
		uploader := s3manager.NewUploader(s)
		if file.GetSize() > s3manager.MaxUploadParts*s3manager.DefaultUploadPartSize {
			uploader.PartSize = file.GetSize() / (s3manager.MaxUploadParts - 1)
		}
		input := &s3manager.UploadInput{
			Bucket: &resp.Data.Bucket,
			Key:    &resp.Data.Key,
			Body: driver.NewLimitedUploadStream(ctx, &driver.ReaderUpdatingProgress{
				Reader:         file,
				UpdateProgress: up,
			}),
		}
		_, err = uploader.UploadWithContext(ctx, input)
		if err != nil {
			return err
		}
	}
	_, err = d.Request(UploadComplete, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"fileId": resp.Data.FileId,
		}).SetContext(ctx)
	}, nil)
	return err
}

func (d *Pan123) APIRateLimit(ctx context.Context, api string) error {
	value, _ := d.apiRateLimit.LoadOrStore(api,
		rate.NewLimiter(rate.Every(700*time.Millisecond), 1))
	limiter := value.(*rate.Limiter)

	return limiter.Wait(ctx)
}

var _ driver.Driver = (*Pan123)(nil)
