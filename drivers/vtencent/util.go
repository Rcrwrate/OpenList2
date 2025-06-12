package vtencent

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/pkg/http_range"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-resty/resty/v2"
)

func (d *Vtencent) setDefaultHeaders(req *resty.Request) {
	req.SetHeaders(map[string]string{
		"cookie":       d.Cookie,
		"content-type": "application/json",
		"origin":       d.conf.origin,
		"referer":      d.conf.referer,
	})
}

func (d *Vtencent) request(url, method string, callback base.ReqCallback, resp any) ([]byte, error) {
	req := base.RestyClient.R()
	d.setDefaultHeaders(req)
	if callback != nil {
		callback(req)
	} else {
		req.SetBody("{}")
	}
	if resp != nil {
		req.SetResult(resp)
	}
	res, err := req.Execute(method, url)
	if err != nil {
		return nil, err
	}
	code := utils.Json.Get(res.Body(), "Code").ToString()
	if code != "Success" {
		if code == "AuthFailure.SessionInvalid" {
			return d.request(url, method, callback, resp)
		}
		return nil, errors.New(code)
	}
	return res.Body(), nil
}

func (d *Vtencent) ugcRequest(url, method string, callback base.ReqCallback, resp interface{}) ([]byte, error) {
	req := base.RestyClient.R()
	d.setDefaultHeaders(req)
	if callback != nil {
		callback(req)
	} else {
		req.SetBody("{}")
	}
	if resp != nil {
		req.SetResult(resp)
	}
	res, err := req.Execute(method, url)
	if err != nil {
		return nil, err
	}
	code := utils.Json.Get(res.Body(), "Code").ToInt()
	if code != 0 {
		message := utils.Json.Get(res.Body(), "message").ToString()
		if message == "" {
			message = utils.Json.Get(res.Body(), "msg").ToString()
		}
		return nil, errors.New(message)
	}
	return res.Body(), nil
}

func (d *Vtencent) LoadUser() (string, error) {
	api := "https://api.vs.tencent.com/SaaS/Account/DescribeAccount"
	res, err := d.request(api, http.MethodPost, func(req *resty.Request) {}, nil)
	if err != nil {
		return "", err
	}
	return utils.Json.Get(res, "Data", "TfUid").ToString(), nil
}

func (d *Vtencent) GetFiles(dirId string) ([]File, error) {
	var res []File
	//offset := 0
	for {
		api := "https://api.vs.tencent.com/PaaS/Material/SearchResource"
		form := fmt.Sprintf(`{
		"Text":"",
		"Text":"",
		"Offset":%d,
		"Limit":50,
		"Sort":{"Field":"%s","Order":"%s"},
		"CreateTimeRanges":[],
		"MaterialTypes":[],
		"ReviewStatuses":[],
		"Tags":[],
		"SearchScopes":[{"Owner":{"Type":"PERSON","Id":"%s"},"ClassId":%s,"SearchOneDepth":true}]
	}`, len(res), d.Addition.OrderBy, d.Addition.OrderDirection, d.TfUid, dirId)
		var resp RspFiles
		_, err := d.request(api, http.MethodPost, func(req *resty.Request) {
			req.SetBody(form).ForceContentType("application/json")
		}, &resp)
		if err != nil {
			return nil, err
		}
		res = append(res, resp.Data.ResourceInfoSet...)
		if len(resp.Data.ResourceInfoSet) <= 0 || len(res) >= resp.Data.TotalCount {
			break
		}
	}
	return res, nil
}

func (d *Vtencent) CreateUploadMaterial(classId int, fileName string, uploadSummaryKey string) (RspCreatrMaterial, error) {
	api := "https://api.vs.tencent.com/PaaS/Material/CreateUploadMaterial"
	form := base.Json{
		"Owner": base.Json{
			"Type": "PERSON",
			"Id":   d.TfUid,
		},
		"MaterialType":     "VIDEO",
		"Name":             fileName,
		"ClassId":          classId,
		"UploadSummaryKey": uploadSummaryKey,
	}

	var resps RspCreatrMaterial
	_, err := d.request(api, http.MethodPost, func(req *resty.Request) {
		req.SetBody(form).ForceContentType("application/json")
	}, &resps)
	if err != nil {
		return RspCreatrMaterial{}, err
	}
	return resps, nil
}

func (d *Vtencent) ApplyUploadUGC(signature string, stream model.FileStreamer) (RspApplyUploadUGC, error) {
	api := "https://vod2.qcloud.com/v3/index.php?Action=ApplyUploadUGC"
	form := base.Json{
		"signature": signature,
		"videoName": stream.GetName(),
		"videoType": utils.Ext(stream.GetName()),
		"videoSize": stream.GetSize(),
	}
	var resps RspApplyUploadUGC
	_, err := d.ugcRequest(api, http.MethodPost, func(req *resty.Request) {
		req.SetBody(form).ForceContentType("application/json")
	}, &resps)
	if err != nil {
		return RspApplyUploadUGC{}, err
	}
	return resps, nil
}

func (d *Vtencent) CommitUploadUGC(signature, vodSessionKey string) (RspCommitUploadUGC, error) {
	api := "https://vod2.qcloud.com/v3/index.php?Action=CommitUploadUGC"
	form := base.Json{
		"signature":     signature,
		"vodSessionKey": vodSessionKey,
	}
	var resps RspCommitUploadUGC
	rsp, err := d.ugcRequest(api, http.MethodPost, func(req *resty.Request) {
		req.SetBody(form).ForceContentType("application/json")
	}, &resps)
	if err != nil {
		return RspCommitUploadUGC{}, err
	}
	if resps.Data.Video.URL == "" {
		return RspCommitUploadUGC{}, errors.New(string(rsp))
	}
	return resps, nil
}

func (d *Vtencent) FinishUploadMaterial(summaryKey, vodVerifyKey, uploadContext, vodFileId string) (RspFinishUpload, error) {
	api := "https://api.vs.tencent.com/PaaS/Material/FinishUploadMaterial"
	form := base.Json{
		"UploadContext": uploadContext,
		"VodVerifyKey":  vodVerifyKey,
		"VodFileId":     vodFileId,
		"UploadFullKey": summaryKey,
	}
	var resps RspFinishUpload
	rsp, err := d.request(api, http.MethodPost, func(req *resty.Request) {
		req.SetBody(form).ForceContentType("application/json")
	}, &resps)
	if err != nil {
		return RspFinishUpload{}, err
	}
	if resps.Data.MaterialID == "" {
		return RspFinishUpload{}, errors.New(string(rsp))
	}
	return resps, nil
}

func (d *Vtencent) FinishHashUploadMaterial(summaryKey, uploadContext string) (RspFinishUpload, error) {
	api := "https://api.vs.tencent.com/PaaS/Material/FinishUploadMaterial"
	form := base.Json{
		"UploadContext": uploadContext,
		"UploadFullKey": summaryKey,
	}
	var resps RspFinishUpload
	rsp, err := d.request(api, http.MethodPost, func(req *resty.Request) {
		req.SetBody(form).ForceContentType("application/json")
	}, &resps)
	if err != nil {
		return RspFinishUpload{}, err
	}
	if resps.Data.MaterialID == "" {
		return RspFinishUpload{}, errors.New(string(rsp))
	}
	return resps, nil
}

func (d *Vtencent) FileUpload(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	classId, err := strconv.Atoi(dstDir.GetID())
	if err != nil {
		return err
	}
	const chunkLength int64 = 1024 * 1024 * 10
	reader, err := stream.RangeRead(http_range.Range{Start: 0, Length: chunkLength})
	if err != nil {
		return err
	}
	chunkHash, err := utils.HashReader(utils.SHA1, reader)
	if err != nil {
		return err
	}
	rspCreateMaterial, err := d.CreateUploadMaterial(classId, stream.GetName(), chunkHash)
	if err != nil {
		return err
	}
	if rspCreateMaterial.Data.QuickUpload {
		summaryKey := stream.GetHash().GetHash(utils.SHA1)
		if len(summaryKey) < utils.SHA1.Width {
			if summaryKey, err = utils.HashReader(utils.SHA1, stream); err != nil {
				return err
			}
		}

		_, err = d.FinishHashUploadMaterial(summaryKey, rspCreateMaterial.Data.UploadContext)
		if err != nil {
			return err
		}
		return nil
	}
	return d.performActualUpload(ctx, stream, rspCreateMaterial, up)
}

func (d *Vtencent) performActualUpload(ctx context.Context, stream model.FileStreamer, rspCreateMaterial RspCreatrMaterial, up driver.UpdateProgress) error {
	hash := sha1.New()
	rspUGC, err := d.ApplyUploadUGC(rspCreateMaterial.Data.VodUploadSign, stream)
	if err != nil {
		return err
	}
	params := rspUGC.Data
	certificate := params.TempCertificate
	cfg := &aws.Config{
		HTTPClient:  base.HttpClient,
		Credentials: credentials.NewStaticCredentials(certificate.SecretID, certificate.SecretKey, certificate.Token),
		Region:      aws.String(params.StorageRegionV5),
		Endpoint:    aws.String(fmt.Sprintf("cos.%s.myqcloud.com", params.StorageRegionV5)),
	}
	ss, err := session.NewSession(cfg)
	if err != nil {
		return err
	}
	uploader := s3manager.NewUploader(ss)
	if stream.GetSize() > s3manager.MaxUploadParts*s3manager.DefaultUploadPartSize {
		uploader.PartSize = stream.GetSize() / (s3manager.MaxUploadParts - 1)
	}
	input := &s3manager.UploadInput{
		Bucket: aws.String(fmt.Sprintf("%s-%d", params.StorageBucket, params.StorageAppID)),
		Key:    &params.Video.StoragePath,
		Body: driver.NewLimitedUploadStream(ctx,
			io.TeeReader(stream, io.MultiWriter(hash, driver.NewProgress(stream.GetSize(), up)))),
	}
	_, err = uploader.UploadWithContext(ctx, input)
	if err != nil {
		return err
	}
	rspCommitUGC, err := d.CommitUploadUGC(rspCreateMaterial.Data.VodUploadSign, rspUGC.Data.VodSessionKey)
	if err != nil {
		return err
	}
	summaryKey := hex.EncodeToString(hash.Sum(nil))
	_, err = d.FinishUploadMaterial(summaryKey, rspCommitUGC.Data.Video.VerifyContent,
		rspCreateMaterial.Data.UploadContext, rspCommitUGC.Data.FileID)
	if err != nil {
		return err
	}
	return nil
}
