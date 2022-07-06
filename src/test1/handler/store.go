package handler

import (
	"go-remon.lc/src/data"
	"go-remon.lc/src/data/config"
	"go-remon.lc/src/data/model"
	"go-remon.lc/src/data/store"

	"github.com/sirupsen/logrus"
	"github.com/tideland/golib/logger"
)

type Test1Store struct {
	OfficersInfo *store.CacheStore
}

func (h *Test1Handler) initStore() error {
	h.store = &Test1Store{}
	OfficersInfo, err := data.NewCacheData(config.Name_Office_Info, h.cache, h.mongo)
	if err != nil {
		logrus.Errorf("data.NewCacheData------: %v %v ", err)
		return err
	}
	h.store.OfficersInfo = OfficersInfo
	return nil
}
func (h *Test1Handler) RunStoreSync() {
	go data.RunSync()
}
func (h *Test1Handler) GetOfficeInfo(uid uint64) (*model.OfficeInfo, error) {
	record, err := h.store.OfficersInfo.Get(uid)
	if err == store.Nil {
		return &model.OfficeInfo{}, store.Nil
	} else if err != nil {
		logger.Errorf("h.store.UserSession.Get------: %v %v ", err, uid)
		return nil, err
	}
	recordset := record.(*model.OfficeInfo)
	return recordset, nil
}

//添加或更改某个玩家的Session
func (h *Test1Handler) UpdateInsertUserOfficeInfo(officedata *model.OfficeInfo) error {
	recordset := &model.OfficeInfo{}
	recordset.Uid = officedata.Uid
	record, err := h.store.OfficersInfo.Get(officedata.Uid)
	if err == nil {
		recordset = record.(*model.OfficeInfo)
	} else if err != store.Nil {
		logger.Errorf("h.store.OfficersInfo.Get------: %v %v ", err, officedata.Uid)
	}

	if officedata.Sex != 0 {
		recordset.Sex = officedata.Sex
	}

	if officedata.Age != 0 {
		recordset.Age = officedata.Age
	}

	if officedata.Position != "" {
		recordset.Position = officedata.Position
	}

	logger.Infof("aaaaaaaaaa")
	err = h.store.OfficersInfo.Set(recordset.Uid, recordset)
	if err != nil {
		logger.Errorf("h.store.OfficersInfo.Set------: %v %v %+v ", err, recordset.Uid, recordset)
		return err
	}
	return nil
}
