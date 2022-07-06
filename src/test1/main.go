package main

import (
	"time"

	"go-remon.lc/src/data/model"
	"go-remon.lc/src/test1/handler"

	"github.com/sirupsen/logrus"
)

func main() {
	h := new(handler.Test1Handler)
	if err := h.Init(); err != nil {
		logrus.Errorf("h.Init------: %v ", err)
		return
	}
	h.RunStoreSync()
	time.Sleep(1 * time.Second)
	officedata := &model.OfficeInfo{Uid: 663692, Age: 26, Sex: 1, Position: "研发工程师"}
	h.UpdateInsertUserOfficeInfo(officedata)

	outofficedata, err := h.GetOfficeInfo(officedata.Uid)
	if err != nil {
		logrus.Errorf("h.GetOfficeInfo------: %v %v ", err, officedata.Uid)
		return
	}

	logrus.Infof("outofficedata------: +v ", outofficedata)
	time.Sleep(60 * time.Minute)
}
