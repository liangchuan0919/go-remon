package model

import "go-remon.lc/src/data/config"

var CfgOfficeInfo = config.DataConfig{
	UniqName:    config.Name_Office_Info,
	ExpireTime:  60 * 60,
	Tdata:       OfficeInfo{},
	DBName:      "test",
	TableName:   "Office_Info",
	SyncTimeout: 1,
}

type OfficeInfo struct {
	Uid      uint64 `bson:"_id" json:"_id"`
	Sex      int32  `bson:"sex" json:"sex"`
	Age      int32  `bson:"age" json:"age"`
	Position string `bson:"position" json:"position"`
}

func (d OfficeInfo) TableName(id interface{}) string {
	return ""
}

func (d OfficeInfo) DefaultData(id interface{}) interface{} {

	return nil
}
