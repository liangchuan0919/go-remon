package store

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type RedisCfg struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	DB   int    `json:"db"`
	Auth string `json:"auth"`
}

type MongoCfg struct {
	Url   string `json:"url"`
	DB    string `json:"db"`
	Table string `json:"table"`
}

// type SetCfg struct { //只有在cache_set里用到
// 	SingleKeys bool  `json:"single_keys"` //true: 表示单键值 set 表，即数据都存在某一个redis集合里。false： 表示多键值 set 表，即数据存在多个redis集合里。
// 	SetType    int32 `json:"set_type"`    // SetType:1  表示单个set中的元素会是200个以上。     SetType:2  表示单个set中的元素会小于200个。
// }
type Options struct {
	uniqName string //全局唯一名字
	redisCfg RedisCfg
	mongoCfg MongoCfg
	// setcfg   SetCfg
	mongo *mongo.Client
	cache *redis.Client

	needSync     bool
	cacheKey     string
	expireSecond int64

	syncTimeout      int64 //距离上一次更新多久 作为落地条件
	syncCountPerTime int64 //每批次落地数量
	syncDisable      bool  //精致同步

	tdata            interface{}
	tdataKind        reflect.Kind
	tdataType        reflect.Type
	tdataIdFieldName string
	tdataIdFieldType reflect.Type

	tkeyKind reflect.Kind
	tkeyType reflect.Type
	//set 默认数据
	tsetDefaultData interface{}
	//元素过期时间
	tsetEleDuration interface{}
}

// Option sets values in Options
type Option func(o *Options)

func Name(a string) Option {
	return func(o *Options) {
		o.uniqName = a
	}
}
func Redis(a *RedisCfg) Option {
	return func(o *Options) {
		o.redisCfg = *a
		o.cache = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", o.redisCfg.Host, o.redisCfg.Port),
			Password: o.redisCfg.Auth,
			DB:       o.redisCfg.DB,
		})
	}
}

func Mongo(a *MongoCfg) Option {
	return func(o *Options) {
		o.mongoCfg = *a
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		//连接池
		connect, err := mongo.Connect(ctx, options.Client().ApplyURI(o.mongoCfg.Url).SetMaxPoolSize(20))
		if err != nil {
			return
		}
		o.mongo = connect
	}
}

func TData(a interface{}) Option {
	return func(o *Options) {
		tp := reflect.TypeOf(a).Kind()
		if tp == reflect.Ptr {
			a = reflect.ValueOf(a).Elem()
			tp = reflect.ValueOf(a).Elem().Kind()
		}
		if tp == reflect.Struct {
			o.tdata = a
			o.tdataKind = reflect.TypeOf(a).Kind()
			o.tdataType = reflect.TypeOf(a)
			for i := 0; i < o.tdataType.NumField(); i++ {
				if "_id" == o.tdataType.Field(i).Tag.Get("bson") {
					o.tdataIdFieldName = o.tdataType.Field(i).Name
					o.tdataIdFieldType = o.tdataType.Field(i).Type
					break
				}
			}
		}
	}
}

func Expired(a int64) Option {
	return func(o *Options) {
		o.expireSecond = a
	}
}

func ReidsClient(a *redis.Client) Option {
	return func(o *Options) {
		o.cache = a
	}
}

func MongoClient(a *mongo.Client) Option {
	return func(o *Options) {
		o.mongo = a
	}
}

func MongoDB(a string) Option {
	return func(o *Options) {
		o.mongoCfg.DB = a
	}
}
func MongoTable(a string) Option {
	return func(o *Options) {
		o.mongoCfg.Table = a
	}
}
func CacheKey(a string) Option {
	return func(o *Options) {
		o.cacheKey = a
	}
}

// func SingleKeys(a bool) Option {
// 	return func(o *Options) {
// 		o.setcfg.SingleKeys = a
// 	}
// }

// func SetType(a int32) Option {
// 	return func(o *Options) {
// 		o.setcfg.SetType = a
// 	}
//}

func TZSetData(a interface{}) Option {
	return func(o *Options) {
		tp := reflect.TypeOf(a).Kind()
		if tp == reflect.Ptr {
			a = reflect.ValueOf(a).Elem()
			tp = reflect.ValueOf(a).Elem().Kind()
		}
		o.tdata = a
		o.tdataKind = reflect.TypeOf(a).Kind()
		o.tdataType = reflect.TypeOf(a)
	}
}

func TMemoryData(key, data interface{}) Option {
	return func(o *Options) {
		if key == nil || data == nil {
			o.tkeyKind = reflect.Invalid
			o.tdataKind = reflect.Invalid
			return
		}
		tp := reflect.TypeOf(key).Kind()
		if tp == reflect.Ptr {
			o.tkeyKind = reflect.Invalid
			o.tdataKind = reflect.Invalid
			return
		}
		o.tkeyKind = reflect.TypeOf(key).Kind()
		o.tkeyType = reflect.TypeOf(key)

		tp = reflect.TypeOf(data).Kind()
		if tp == reflect.Ptr {
			data = reflect.ValueOf(data).Elem()
			if reflect.TypeOf(data).Kind() != reflect.Struct {
				o.tdataKind = reflect.Invalid
				return
			}
		}
		o.tdata = data
		o.tdataKind = reflect.TypeOf(data).Kind()
		o.tdataType = reflect.TypeOf(data)
	}
}

func TSetData(a interface{}) Option {
	return func(o *Options) {
		tp := reflect.TypeOf(a).Kind()
		if tp == reflect.Ptr {
			a = reflect.ValueOf(a).Elem()
		}
		o.tdata = a
		o.tdataKind = reflect.TypeOf(a).Kind()
		o.tdataType = reflect.TypeOf(a)
	}
}

func TSetDefaultData(a interface{}) Option {
	return func(o *Options) {
		o.tsetDefaultData = a
	}
}

func TSetEleDuration(a interface{}) Option {
	return func(o *Options) {
		o.tsetEleDuration = a
	}
}

func SyncParam(disable bool, timeout, count int64) Option {
	return func(o *Options) {
		o.syncDisable = disable
		o.syncTimeout = timeout
		o.syncCountPerTime = count
	}
}
