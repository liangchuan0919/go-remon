package data

import (
	"go-remon.lc/src/data/config"
	"go-remon.lc/src/data/model"
)

var dataConfigList = map[string]*config.DataConfig{
	config.Name_Office_Info: &model.CfgOfficeInfo,
}

// var zsetDataConfigList = map[string]*config.ZSetDataConfig{
// 	config.Name_user_recommend_t1: &model.CfgUserRecommendT1,
// }

// var setDataConfigList = map[string]*config.SetDataConfig{
// 	config.Name_user_face_rg: &model.CfgUserFaceRg,
// }

// var memoryDataConfigList = map[string]*config.MemoryDataConfig{
// 	config.Name_city_match_t1: &model.CfgCityMatchT1,
// }

func GetDataConfig(name string) *config.DataConfig {
	if r, ok := dataConfigList[name]; ok {
		return r
	}
	return nil
}

// func GetZSetDataConfig(name string) *config.ZSetDataConfig {
// 	if r, ok := zsetDataConfigList[name]; ok {
// 		return r
// 	}
// 	return nil
// }

// func GetMemoryDataConfig(name string) *config.MemoryDataConfig {
// 	if r, ok := memoryDataConfigList[name]; ok {
// 		return r
// 	}
// 	return nil
// }

// func GetSetDataConfig(name string) *config.SetDataConfig {
// 	if r, ok := setDataConfigList[name]; ok {
// 		return r
// 	}
// 	return nil
// }
