package handles

import (
	"errors"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/sharing"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type SharingListReq struct {
	model.PageReq
	SharingId string `json:"sid" form:"sid"`
	Path      string `json:"path" form:"path"`
	Pwd       string `json:"pwd" form:"pwd"`
	Refresh   bool   `json:"refresh"`
}

type SharingGetReq struct {
	SharingId string `json:"sid" form:"sid"`
	Path      string `json:"path" form:"path"`
	Pwd       string `json:"pwd" form:"pwd"`
}

type SharingObjResp struct {
	Name        string                     `json:"name"`
	Size        int64                      `json:"size"`
	IsDir       bool                       `json:"is_dir"`
	Modified    time.Time                  `json:"modified"`
	Created     time.Time                  `json:"created"`
	HashInfoStr string                     `json:"hashinfo"`
	HashInfo    map[*utils.HashType]string `json:"hash_info"`
}

func SharingGet(c *gin.Context) {
	var req SharingGetReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	obj, err := sharing.Get(c.Request.Context(), req.SharingId, req.Path, model.SharingListArgs{
		Refresh: false,
		Pwd:     req.Pwd,
	})
	if dealError(c, err) {
		return
	}
	common.SuccessResp(c, SharingObjResp{
		Name:        obj.GetName(),
		Size:        obj.GetSize(),
		IsDir:       obj.IsDir(),
		Modified:    obj.ModTime(),
		Created:     obj.CreateTime(),
		HashInfoStr: obj.GetHash().String(),
		HashInfo:    obj.GetHash().Export(),
	})
}

func SharingList(c *gin.Context) {
	var req SharingListReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	objs, err := sharing.List(c.Request.Context(), req.Pwd, req.Path, model.SharingListArgs{
		Refresh: req.Refresh,
		Pwd:     req.Pwd,
	})
	if dealError(c, err) {
		return
	}
	total, objs := pagination(objs, &req.PageReq)
	common.SuccessResp(c, common.PageResp{
		Content: utils.MustSliceConvert(objs, func(obj model.Obj) ObjResp {
			return ObjResp{
				Name:        obj.GetName(),
				Size:        obj.GetSize(),
				IsDir:       obj.IsDir(),
				Modified:    obj.ModTime(),
				Created:     obj.CreateTime(),
				HashInfoStr: obj.GetHash().String(),
				HashInfo:    obj.GetHash().Export(),
			}
		}),
		Total: int64(total),
	})
}

func SharingDown(c *gin.Context) {
	sid := c.Request.Context().Value(conf.SharingIDKey).(string)
	path := c.Request.Context().Value(conf.PathKey).(string)
	pwd := c.Query("pwd")
	storage, link, obj, err := sharing.Link(c.Request.Context(), sid, path, &sharing.LinkArgs{
		SharingListArgs: model.SharingListArgs{
			Pwd: pwd,
		},
		LinkArgs: model.LinkArgs{
			Header: c.Request.Header,
			Type:   c.Query("type"),
		},
	})
	if dealError(c, err) {
		return
	}
	proxy(c, link, obj, storage.GetStorage().ProxyRange)
}

func dealError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	} else if errors.Is(err, errs.SharingNotFound) {
		common.ErrorResp(c, err, 404)
	} else if errors.Is(err, errs.InvalidSharing) {
		common.ErrorResp(c, err, 410)
	} else if errors.Is(err, errs.WrongShareCode) {
		common.ErrorResp(c, err, 403)
	} else {
		common.ErrorResp(c, err, 500)
	}
	return true
}

type SharingResp struct {
	*model.Sharing
	CreatorName string `json:"creator"`
	CreatorRole int    `json:"creator_role"`
}

func ListSharings(c *gin.Context) {
	var req model.PageReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	var sharings []model.Sharing
	var total int64
	var err error
	if user.IsAdmin() {
		sharings, total, err = op.GetSharings(req.Page, req.PerPage)
	} else {
		sharings, total, err = op.GetSharingsByCreatorId(user.ID, req.Page, req.PerPage)
	}
	if err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c, common.PageResp{
		Content: utils.MustSliceConvert(sharings, func(s model.Sharing) SharingResp {
			return SharingResp{
				Sharing: &s,
				CreatorName: s.Creator.Username,
				CreatorRole: s.Creator.Role,
			}
		}),
		Total:   total,
	})
}

type CreateSharingReq struct {
	Files       []string  `json:"files"`
	Expires     time.Time `json:"expires"`
	Pwd         string    `json:"pwd"`
	MaxAccessed int       `json:"max_accessed"`
	Disabled    bool      `json:"disabled"`
	model.Sort
}

type UpdateSharingReq struct {
	ID       string `json:"id"`
	Accessed int    `json:"accessed"`
	CreateSharingReq
}

func UpdateSharing(c *gin.Context) {
	var req UpdateSharingReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if !user.CanShare() {
		common.ErrorStrResp(c, "permission denied", 403)
		return
	}
	s, err := op.GetSharingById(req.ID)
	if err != nil || (!user.IsAdmin() && s.CreatorId != user.ID) {
		common.ErrorStrResp(c, "sharing not found", 404)
		return
	}
	s.Files = req.Files
	s.Expires = req.Expires
	s.Pwd = req.Pwd
	s.Accessed = req.Accessed
	s.MaxAccessed = req.MaxAccessed
	s.Disabled = req.Disabled
	s.Sort = req.Sort
	if err = op.UpdateSharing(s); err != nil {
		common.ErrorResp(c, err, 500)
	} else {
		common.SuccessResp(c)
	}
}

func CreateSharing(c *gin.Context) {
	var req CreateSharingReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if !user.CanShare() {
		common.ErrorStrResp(c, "permission denied", 403)
		return
	}
	s := &model.Sharing{
		SharingDB: &model.SharingDB{
			Expires:     req.Expires,
			Pwd:         req.Pwd,
			Accessed:    0,
			MaxAccessed: req.MaxAccessed,
			Disabled:    req.Disabled,
			Sort:        req.Sort,
		},
		Files:     req.Files,
		Creator:   user,
	}
	if err := op.CreateSharing(s); err != nil {
		common.ErrorResp(c, err, 500)
	} else {
		common.SuccessResp(c)
	}
}

func DeleteSharing(c *gin.Context) {
	sid := c.Query("id")
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	s, err := op.GetSharingById(sid)
	if err != nil || (!user.IsAdmin() && s.CreatorId != user.ID) {
		common.ErrorStrResp(c, "sharing not found", 404)
		return
	}
	if err = op.DeleteSharing(sid); err != nil {
		common.ErrorResp(c, err, 500)
	} else {
		common.SuccessResp(c)
	}
}
