package db

import (
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils/random"
	"github.com/pkg/errors"
)

func GetSharingById(id string) (*model.SharingDB, error) {
	var s model.SharingDB
	if err := db.First(&s, id).Error; err != nil {
		return nil, errors.Wrapf(err, "failed get sharing")
	}
	return &s, nil
}

func GetSharings(pageIndex, pageSize int) (sharings []model.SharingDB, count int64, err error) {
	sharingDB := db.Model(&model.SharingDB{})
	if err := sharingDB.Count(&count).Error; err != nil {
		return nil, 0, errors.Wrapf(err, "failed get sharings count")
	}
	if err := sharingDB.Order(columnName("id")).Offset((pageIndex - 1) * pageSize).Limit(pageSize).Find(&sharings).Error; err != nil {
		return nil, 0, errors.Wrapf(err, "failed get find sharings")
	}
	return sharings, count, nil
}

func GetSharingsByCreatorId(creator uint, pageIndex, pageSize int) (sharings []model.SharingDB, count int64, err error) {
	sharingDB := db.Model(&model.SharingDB{})
	cond := model.SharingDB{CreatorId: creator}
	if err := sharingDB.Where(cond).Count(&count).Error; err != nil {
		return nil, 0, errors.Wrapf(err, "failed get sharings count")
	}
	if err := sharingDB.Where(cond).Order(columnName("id")).Offset((pageIndex - 1) * pageSize).Limit(pageSize).Find(&sharings).Error; err != nil {
		return nil, 0, errors.Wrapf(err, "failed get find sharings")
	}
	return sharings, count, nil
}

func CreateSharing(s *model.SharingDB) error {
	id := random.String(8)
	for len(id) < 20 {
		var unused model.SharingDB
		if err := db.First(&unused, id).Error; err != nil {
			s.ID = id
			return errors.WithStack(db.Create(s).Error)
		}
		id += random.String(1)
	}
	return errors.New("failed find valid id")
}

func UpdateSharing(s *model.SharingDB) error {
	return errors.WithStack(db.Save(s).Error)
}

func DeleteSharingById(id string) error {
	return errors.WithStack(db.Delete(&model.SharingDB{}, id).Error)
}
