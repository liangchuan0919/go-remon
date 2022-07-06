package config

type DataConfig struct {
	UniqName    string      `json:"uniq_name"`
	ExpireTime  int64       `json:"expire_time"`
	Tdata       interface{} `json:"tdata"`
	DBName      string      `json:"db_name"`
	TableName   string      `json:"table_name"`
	CacheKey    string      `json:"cache_key"`
	SyncTimeout int64       `json:"sync_timeout"`
	SyncCount   int64       `json:"sync_count"`
	SyncDisable bool        `json:"sync_disable"`
}

const (
	Name_Office_Info = "office_info"
)
