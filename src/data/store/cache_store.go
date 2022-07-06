package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const DEFAULT_SYNC_TIMEOUT = 60
const DEFAULT_SYNC_COUNT = 200
const DEFAULT_SYNC_COUNT_MAX = 2000

const SYNC_HASH_END_DATE = "2021-12-15"

type ITData interface {
	TableName(id interface{}) string
	DefaultData(id interface{}) interface{}
}

type CacheStore struct {
	options          Options
	syncCountPerTime int64 //每批次落地数量
}

func (d *CacheStore) Init(opts ...Option) error {
	for _, o := range opts {
		o(&d.options)
	}
	if d.options.uniqName == "" {
		msg := "CacheDBStore Init Err uniqName"
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.cacheKey == "" {
		d.options.cacheKey = d.options.uniqName
	}
	if d.options.expireSecond <= 0 {
		msg := "CacheDBStore Init Err expireSecond name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.syncTimeout <= 0 {
		d.options.syncTimeout = DEFAULT_SYNC_TIMEOUT
	}
	if d.options.syncCountPerTime <= 0 {
		d.options.syncCountPerTime = DEFAULT_SYNC_COUNT
	}
	d.syncCountPerTime = d.options.syncCountPerTime

	if d.options.tdata == nil {
		msg := "CacheDBStore Init Err tdata nil name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if _, ok := d.options.tdata.(ITData); !ok {
		msg := "CacheDBStore Init Err tdata typenot Data name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.tdataKind != reflect.Struct {
		msg := "CacheDBStore Init Err tdataKind no reflect.Struct name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.tdataIdFieldName == "" {
		msg := "CacheDBStore Init Err tdataIdFieldName empty name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}

	if d.options.cache == nil {
		msg := "CacheDBStore Init Err cache nil expireSecond name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.mongo == nil {
		d.options.needSync = false
		msg := "CacheDBStore Init mongo nil name:" + d.options.uniqName
		logrus.Error(msg)
	} else {
		if d.options.mongoCfg.DB == "" {
			msg := "CacheDBStore Init mongoCfg DB nil name:" + d.options.uniqName
			logrus.Error(msg)
			return errors.New(msg)
		}
		if d.options.mongoCfg.Table == "" {
			msg := "CacheDBStore Init mongoCfg Table nil name:" + d.options.uniqName
			logrus.Error(msg)
			return errors.New(msg)
		}
		if d.options.tdataKind == reflect.Struct {
			d.options.needSync = true
		}
	}
	if d.options.syncDisable {
		d.options.needSync = false
	}
	return nil
}
func (d *CacheStore) Name() string {
	return d.options.uniqName
}
func (d *CacheStore) Get(id interface{}) (rst interface{}, err error) {
	rst, err = d.getEx(id, true)
	if err == redis.Nil || err == mongo.ErrNoDocuments {
		err = Nil
	}
	return
}
func (d *CacheStore) Set(id, value interface{}) error {
	return d.setEx(id, value, false, false)
}
func (d *CacheStore) SetAlways(id, value interface{}) error {
	return d.setEx(id, value, true, false)
}
func (d *CacheStore) Read(id interface{}) (interface{}, error) {
	if d.options.mongo == nil {
		msg := fmt.Sprintf("read Err:no mongo name:%v id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	if d.options.tdataKind != reflect.Struct {
		msg := fmt.Sprintf("read Err:invalid tdata kind no struct name:%v id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	if id == nil {
		msg := fmt.Sprintf("read Err:id nil name:%v id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	if d.options.tdataIdFieldType != reflect.TypeOf(id) {
		msg := fmt.Sprintf("read Err:id type error name:%v id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	tableName := d.getTableName(id)
	db := d.options.mongo.Database(d.options.mongoCfg.DB)
	coll := db.Collection(tableName)
	filter := bson.M{"_id": id}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	result := coll.FindOne(ctx, filter)
	if result.Err() == mongo.ErrNoDocuments {
		//msg := fmt.Sprintf("read Err:no document name:%v id:%v", d.options.uniqName, id)
		//logrus.Error(msg)
		return nil, Nil
	}
	if result.Err() != nil {
		msg := fmt.Sprintf("read Err:%v name:%v id:%v", result.Err().Error(), d.options.uniqName, id)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	var ii = reflect.New(d.options.tdataType)
	rst := ii.Interface()
	err := result.Decode(rst)
	logrus.Infof("Read name=%v id=%v value:%v err:%v", d.options.uniqName, id, rst, err)
	if err != nil {
		msg := fmt.Sprintf("read Err decode:%v name:%v id:%v", err.Error(), d.options.uniqName, id)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	return rst, nil
}
func (d *CacheStore) Save(id, value interface{}) error {
	if d.options.tdataKind != reflect.Struct {
		msg := fmt.Sprintf("save Err:invalid tdata kind no struct name:%v,id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.mongo == nil {
		msg := fmt.Sprintf("save Err:no db  name:%v,id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return errors.New(msg)
	}
	if id == nil || d.options.tdataIdFieldType != reflect.TypeOf(id) {
		msg := fmt.Sprintf("save Err:id type error name:%v id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return errors.New(msg)
	}

	switch reflect.ValueOf(value).Kind() {
	case reflect.Ptr:
	case reflect.Struct:
	default:
		msg := fmt.Sprintf("save Err:type name:%v,id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return errors.New(msg)
	}

	tableName := d.getTableName(id)
	db := d.options.mongo.Database(d.options.mongoCfg.DB)
	coll := db.Collection(tableName)
	filter := bson.M{"_id": id}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": value}).Err()
	if err != nil {
		_, err := coll.InsertOne(ctx, value)
		//logrus.Infof("Save insert name=%v id=%v value:%v err:%v", d.options.uniqName, id, value, err)
		if err != nil {
			msg := fmt.Sprintf("Save Err:insert:%v name:%v id:%v", err.Error(), d.options.uniqName, id)
			logrus.Error(msg)
			return errors.New(msg)
		}
	} else {
		//logrus.Infof("Save update name=%v id=%v value:%v err:%v", d.options.uniqName, id, value, err)
	}
	return nil
}
func (d *CacheStore) Sync() error {
	if !d.options.needSync {
		return nil
	}
	if d.options.mongo == nil {
		msg := fmt.Sprintf("Sync Err:no db name:%v", d.options.uniqName)
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.tdataKind != reflect.Struct {
		msg := fmt.Sprintf("Sync Err:invalid tdata kind no struct name:%v", d.options.uniqName)
		logrus.Error(msg)
		return errors.New(msg)
	}
	//syncKey := d.getSyncKey()
	//ssm, err := d.options.cache.HGetAll(context.Background(), syncKey).Result()
	total, need, ssm, err := d.getSyncItems()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		msg := fmt.Sprintf("Sync Err:get all key name:%v err:%v", d.options.uniqName, err)
		logrus.Error(msg)
		return err
	}
	allCount := 0
	newCount := 0
	for key, version := range ssm {
		insert, err := d.syncOne(key, version)
		if err != nil {
			msg := fmt.Sprintf("Sync Err:syncOne name:%v key:%v err:%v", d.options.uniqName, key, err)
			logrus.Error(msg)
			continue
		}
		allCount++
		newCount += insert
		logrus.Infof("Sync succ key:%v version:%v", key, version)
	}
	logrus.Errorf("Sync succ name:%v total:%v need:%v all:%v new:%v", d.options.uniqName, total, need, allCount, newCount)

	if time.Now().Format("2006-01-02") <= SYNC_HASH_END_DATE {
		d.syncHash()
	}
	return nil
}

func (d *CacheStore) syncHash() error {
	if !d.options.needSync {
		return nil
	}
	if d.options.mongo == nil {
		msg := fmt.Sprintf("SyncHash Err:no db name:%v", d.options.uniqName)
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.tdataKind != reflect.Struct {
		msg := fmt.Sprintf("SyncHash Err:invalid tdata kind no struct name:%v", d.options.uniqName)
		logrus.Error(msg)
		return errors.New(msg)
	}
	//syncKey := d.getSyncKey()
	//ssm, err := d.options.cache.HGetAll(context.Background(), syncKey).Result()
	total, need, ssm, err := d.getSyncItemsHash()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		msg := fmt.Sprintf("SyncHash Err:get all key name:%v err:%v", d.options.uniqName, err)
		logrus.Error(msg)
		return err
	}
	allCount := 0
	newCount := 0
	for key, version := range ssm {
		insert, err := d.syncOneHash(key, version)
		if err != nil {
			msg := fmt.Sprintf("SyncHash Err:syncOne name:%v key:%v err:%v", d.options.uniqName, key, err)
			logrus.Error(msg)
			continue
		}
		allCount++
		newCount += insert
		logrus.Infof("SyncHash succ key:%v version:%v", key, version)
	}
	logrus.Errorf("SyncHash succ name:%v total:%v need:%v all:%v new:%v", d.options.uniqName, total, need, allCount, newCount)
	return nil
}

func (d *CacheStore) ReadMany(tableName string, filter interface{}, opt interface{}) (interface{}, error) {
	if d.options.tdataKind != reflect.Struct {
		msg := fmt.Sprintf("ReadMany Err:invalid tdata kind no struct name:%v", d.options.uniqName)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	if d.options.mongo == nil {
		msg := fmt.Sprintf("ReadMany Err:no db filter name:%v", d.options.uniqName)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	findOpt := options.Find()
	if opt != nil {
		tmpOpt, ok := opt.(*options.FindOptions)
		if ok {
			findOpt = tmpOpt
		} else {
			msg := fmt.Sprintf("ReadMany Err:no opt FindOptions name:%v", d.options.uniqName)
			logrus.Error(msg)
			return nil, errors.New(msg)
		}
	}

	if tableName == "" {
		tableName = d.options.mongoCfg.Table
	}

	stp := reflect.SliceOf(reflect.PtrTo(d.options.tdataType))
	var ii = reflect.New(stp)

	db := d.options.mongo.Database(d.options.mongoCfg.DB)
	coll := db.Collection(tableName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	cursor, err := coll.Find(ctx, filter, findOpt)
	if err == mongo.ErrNoDocuments {
		return ii.Elem().Interface(), nil
	}
	if err != nil {
		msg := fmt.Sprintf("ReadMany Err:%v name:%v filter:%v", err, d.options.uniqName, filter)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	defer cursor.Close(ctx)
	rst := ii.Interface()
	err = cursor.All(ctx, rst)
	if err != nil {
		msg := fmt.Sprintf("ReadMany decode Err:%v name:%v filter:%v", err, d.options.uniqName, filter)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	return ii.Elem().Interface(), nil

}
func (d *CacheStore) IsNeedSync() bool {
	return d.options.needSync
}
func (d *CacheStore) Remove(id interface{}) error {
	if id == nil {
		msg := fmt.Sprintf("Remove Err name=%v id nil", d.options.uniqName)
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.tdataIdFieldType != reflect.TypeOf(id) {
		msg := fmt.Sprintf("Remove Err name=%v id type err", d.options.uniqName)
		logrus.Error(msg)
		return errors.New(msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	key := d.getCacheKey(id)
	d.options.cache.Del(ctx, key)

	if d.options.needSync {
		d.delSyncItem(key)
	}

	tableName := d.getTableName(id)
	db := d.options.mongo.Database(d.options.mongoCfg.DB)
	coll := db.Collection(tableName)
	filter := bson.M{"_id": id}
	_, err := coll.DeleteOne(ctx, filter)
	if err == mongo.ErrNoDocuments {
		msg := fmt.Sprintf("Remove Err:no document name=%v id:%v", d.options.uniqName, id)
		logrus.Error(msg)
		return nil
	} else if err != nil {
		msg := fmt.Sprintf("Remove Err: name=%v id:%v err:%v", d.options.uniqName, id, err)
		logrus.Error(msg)
		return err
	}
	return nil
}

func (d *CacheStore) convertRedisData(val string) (interface{}, error) {
	switch d.options.tdataKind {
	case reflect.Struct:
		var ii = reflect.New(d.options.tdataType)
		rst := ii.Interface()
		err := json.Unmarshal([]byte(val), rst)
		return rst, err
	case reflect.String:
		return val, nil
	case reflect.Int:
	case reflect.Int32:
	case reflect.Int64:
		i, err := strconv.ParseInt(val, 10, 64)
		return i, err
	case reflect.Uint:
	case reflect.Uint32:
	case reflect.Uint64:
		i, err := strconv.ParseUint(val, 10, 64)
		return i, err
	case reflect.Float32:
	case reflect.Float64:
		i, err := strconv.ParseFloat(val, 64)
		return i, err
	}
	return nil, errors.New("convert err")
}
func (d *CacheStore) getEx(id interface{}, readIf bool) (interface{}, error) {
	if id == nil {
		msg := fmt.Sprintf("Get name=%v id nil", d.options.uniqName)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	if d.options.tdataIdFieldType != reflect.TypeOf(id) {
		msg := fmt.Sprintf("Get name=%v id type err", d.options.uniqName)
		logrus.Error(msg)
		return nil, errors.New(msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	key := d.getCacheKey(id)
	val, err := d.options.cache.Get(ctx, key).Result()
	if err == redis.Nil {
		if !readIf {
			return nil, Nil
		}
		//提前判断
		if d.options.mongo == nil {
			rst := d.options.tdata.(ITData).DefaultData(id)
			if rst == nil {
				//msg := fmt.Sprintf("getEx err DefaultData name=%v key=%v id=%v", d.options.uniqName, key, id)
				//logrus.Error(msg)
				return rst, Nil
			} else {
				logrus.Infof("getEx DefaultData name=%v key=%v id=%v", d.options.uniqName, key, id)
				//默认数据不落地
				errSet := d.setEx(id, rst, false, true)
				if errSet != nil {
					msg := fmt.Sprintf("getEx err Set DefaultData name=%v key=%v id=%v", d.options.uniqName, key, id)
					logrus.Error(msg)
				}
				return rst, nil
			}
		}
		rst, err := d.Read(id)
		if err == nil {
			errSet := d.Set(id, rst)
			if errSet != nil {
				msg := fmt.Sprintf("getEx err Set name=%v key=%v id=%v", d.options.uniqName, key, id)
				logrus.Error(msg)
			}
			return rst, err
		} else if err == Nil {
			rst = d.options.tdata.(ITData).DefaultData(id)
			if rst == nil {
				//msg := fmt.Sprintf("getEx err DefaultData name=%v key=%v id=%v", d.options.uniqName, key, id)
				//logrus.Error(msg)
				return rst, Nil
			} else {
				logrus.Infof("getEx DefaultData name=%v key=%v id=%v", d.options.uniqName, key, id)
				//默认数据不落地
				errSet := d.setEx(id, rst, false, true)
				if errSet != nil {
					msg := fmt.Sprintf("getEx err Set DefaultData name=%v key=%v id=%v", d.options.uniqName, key, id)
					logrus.Error(msg)
				}
				return rst, nil
			}
		} else {
			msg := fmt.Sprintf("getEx err read name=%v key=%v id=%v err=%v", d.options.uniqName, key, id, err)
			logrus.Error(msg)
			return nil, err
		}
	} else if err != nil {
		logrus.Errorf("getEx err name=%v key=%v value=%v err=[%v]", d.options.uniqName, key, val, err)
		return nil, err
	} else {
		return d.convertRedisData(val)
	}
}

func (d *CacheStore) getByKeyForSync(key string) (rst interface{}, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	val, err := d.options.cache.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	} else {
		return d.convertRedisData(val)
	}
}

func (d *CacheStore) setEx(id, value interface{}, noExpired bool, noSync bool) error {
	if id == nil {
		msg := "setEx err id nil name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if d.options.tdataIdFieldType != reflect.TypeOf(id) {
		msg := "setEx err id type err name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	if value == nil {
		msg := "setEx err value nil name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	saveData := ""
	switch reflect.ValueOf(value).Kind() {
	case reflect.Ptr:
		if d.options.tdataType == reflect.ValueOf(value).Type().Elem() {
			bs, err := json.Marshal(value)
			if err != nil {
				return err
			}
			saveData = string(bs)
		} else {
			msg := "setEx err ptr value type!=tdataType name:" + d.options.uniqName
			logrus.Error(msg)
			return errors.New(msg)
		}
	case reflect.Struct:
		if d.options.tdataType != reflect.ValueOf(value).Type() {
			msg := "setEx err value type!=tdataType name:" + d.options.uniqName
			logrus.Error(msg)
			return errors.New(msg)
		}
		bs, err := json.Marshal(value)
		if err != nil {
			return err
		}
		saveData = string(bs)
	case reflect.String:
		saveData = value.(string)
	default:
		msg := "setEx err value type err name:" + d.options.uniqName
		logrus.Error(msg)
		return errors.New(msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	key := d.getCacheKey(id)
	if noExpired {
		err := d.options.cache.Set(ctx, key, saveData, redis.KeepTTL).Err()
		if err != nil {
			logrus.Errorf("setEx err keepttl name=%v key=%v valule=%v err:%v", d.options.uniqName, key, saveData, err)
		}
		if noSync {
			return err
		}
		if err == nil {
			d.setSync(key)
		}
		return err
	} else {
		err := d.options.cache.Set(ctx, key, saveData, time.Duration(d.options.expireSecond)*time.Second).Err()
		if err != nil {
			logrus.Errorf("setEx err name=%v key=%v valule=%v err:%v", d.options.uniqName, key, saveData, err)
		}
		if noSync {
			return err
		}
		if err == nil {
			d.setSync(key)
		}
		return err
	}
}
func (d *CacheStore) getTableName(id interface{}) string {
	nn := ""
	ii, ok := d.options.tdata.(ITData)
	if ok {
		nn = ii.TableName(id)
	}
	if nn == "" {
		nn = d.options.mongoCfg.Table
	}
	return nn
}
func (d *CacheStore) getCacheKey(id interface{}) string {
	key := fmt.Sprintf("%s:%v", d.options.cacheKey, id)
	return key
}

func (d *CacheStore) getSyncKeyHash() string {
	syncKey := fmt.Sprintf("SYNC:%s", d.options.uniqName)
	return syncKey
}
func (d *CacheStore) setSyncHash(key string) error {
	if !d.options.needSync {
		return nil
	}
	if nil == d.options.mongo {
		msg := fmt.Sprintf("setSync Err db nil name:%v,key:%v", d.options.uniqName, key)
		logrus.Error(msg)
		return errors.New(msg)
	}
	syncKey := d.getSyncKeyHash()
	_, err := d.options.cache.HIncrBy(context.Background(), syncKey, key, 1).Result()
	if err != nil {
		msg := fmt.Sprintf("setSync Err HIncrBy name:%v,key:%v err:%v", d.options.uniqName, key, err)
		logrus.Error(msg)
		return errors.New(msg)
	}
	return err
}
func (d *CacheStore) delSyncItemHash(key string) error {
	syncKey := d.getSyncKeyHash()
	return d.options.cache.HDel(context.Background(), syncKey, key).Err()
}
func (d *CacheStore) getSyncItemsHash() (int64, int64, map[string]string, error) {
	syncKey := d.getSyncKeyHash()
	ssm, err := d.options.cache.HGetAll(context.Background(), syncKey).Result()
	if err != nil {
		return 0, 0, ssm, err
	}
	count := int64(len(ssm))
	return count, count, ssm, err
}
func (d *CacheStore) syncOneHash(key string, version string) (int, error) {
	rst, err := d.getByKeyForSync(key)
	if err != nil {
		return 0, err
	}
	//--------------------------------------------------------------------------------
	//从指针转成结构体类型
	if _, ok := reflect.TypeOf(rst).Elem().FieldByName(d.options.tdataIdFieldName); !ok {
		msg := fmt.Sprintf("syncOne fail no id field :%v", key)
		logrus.Error(msg)
		return 0, errors.New(msg)
	}
	empty := reflect.Value{}
	//从指针转成结构体类型
	val := reflect.ValueOf(rst).Elem().FieldByName(d.options.tdataIdFieldName)
	if val == empty {
		msg := fmt.Sprintf("syncOne fail id field empty value:%v", key)
		logrus.Error(msg)
		return 0, errors.New(msg)
	}
	//--------------------------------------------------------------------------------
	filter := bson.M{"_id": val.Interface()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	insert := int(0)
	tableName := d.getTableName(val.Interface())
	coll := d.options.mongo.Database(d.options.mongoCfg.DB).Collection(tableName)
	err = coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": rst}).Err()
	if err != nil {
		_, err = coll.InsertOne(ctx, rst)
		if err != nil {
			msg := fmt.Sprintf("syncOne insert Err key:%v err:%v", key, err.Error())
			logrus.Error(msg)
			return 0, errors.New(msg)
		}
		insert = 1
	}
	logrus.Infof("syncOne name=%v key=%v id=%v value:%v err:%v", d.options.uniqName, key, val.Interface(), rst, err)
	syncKey := d.getSyncKeyHash()
	_, err = d.options.cache.Eval(ctx, syncCacheScript, []string{syncKey}, key, version).Int()
	if err != nil {
		msg := fmt.Sprintf("syncOne fail to eval name:%v key:%v err:%v", d.options.uniqName, key, err)
		logrus.Error(msg)
		return insert, errors.New(msg)
	}
	return insert, nil
}

func (d *CacheStore) getSyncKeyZSet() string {
	syncKey := fmt.Sprintf("SYNC_ZSET:%s", d.options.uniqName)
	return syncKey
}
func (d *CacheStore) setSyncZSet(key string) error {
	if !d.options.needSync {
		return nil
	}
	if nil == d.options.mongo {
		msg := fmt.Sprintf("setSync Err db nil name:%v,key:%v", d.options.uniqName, key)
		logrus.Error(msg)
		return errors.New(msg)
	}
	syncKey := d.getSyncKeyZSet()
	//毫秒级
	score := time.Now().UnixNano() / int64(time.Millisecond)
	memberAdd := &redis.Z{
		Score:  float64(score),
		Member: key,
	}
	memberInc := &redis.Z{
		Score:  1,
		Member: key,
	}
	//ZAdd 会导致刷新频繁数据 一直得不到落地机会
	//ZAddNX 存在score不变 可能会丢失最后一次数据 syncCacheScriptByScore
	//ZIncrXX 旧数据每次加1毫秒 新数据不处理
	err := d.options.cache.ZIncrXX(context.Background(), syncKey, memberInc).Err()
	if err != nil {
		msg := fmt.Sprintf("setSync step ZIncrXX name:%v,key:%v err:%v", d.options.uniqName, key, err)
		logrus.Info(msg)
		err = d.options.cache.ZAdd(context.Background(), syncKey, memberAdd).Err()
		if err != nil {
			msg := fmt.Sprintf("setSync Err ZAdd name:%v,key:%v err:%v", d.options.uniqName, key, err)
			logrus.Error(msg)
			return errors.New(msg)
		}
	}
	return err
}
func (d *CacheStore) delSyncItemZSet(key string) error {
	syncKey := d.getSyncKeyZSet()
	return d.options.cache.ZRem(context.Background(), syncKey, key).Err()
}
func (d *CacheStore) getSyncItemsZSet() (int64, int64, map[string]string, error) {
	count := d.syncCountPerTime
	if count == 0 {
		count = d.options.syncCountPerTime
	}
	syncKey := d.getSyncKeyZSet()
	min := "0"
	score := time.Now().UnixNano()/int64(time.Millisecond) - d.options.syncTimeout*1000
	max := strconv.FormatInt(score, 10)
	ctx := context.Background()
	dataList, err := d.options.cache.ZRangeByScoreWithScores(ctx, syncKey, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  count,
	}).Result()
	if err != nil {
		return 0, 0, map[string]string{}, err
	}
	mmap := map[string]string{}
	for _, item := range dataList {
		key := fmt.Sprintf("%v", item.Member)
		score := strconv.FormatInt(int64(item.Score), 10)
		mmap[key] = score
	}
	all, _ := d.options.cache.ZCard(ctx, syncKey).Result()
	need, _ := d.options.cache.ZCount(ctx, syncKey, min, max).Result()
	//------------------------------------------
	//----调节数量----
	if need > count {
		d.syncCountPerTime = count + d.options.syncCountPerTime
		if d.syncCountPerTime > DEFAULT_SYNC_COUNT_MAX {
			d.syncCountPerTime = DEFAULT_SYNC_COUNT_MAX
		}
	} else {
		d.syncCountPerTime = count - d.options.syncCountPerTime
		if d.syncCountPerTime < d.options.syncCountPerTime {
			d.syncCountPerTime = d.options.syncCountPerTime
		}
	}
	//msg := fmt.Sprintf("getSyncItemsZSet count %v:%v name:%v", d.syncCountPerTime, d.options.syncCountPerTime, d.options.uniqName)
	//logrus.Errorf(msg)
	//------------------------------------------
	return all, need, mmap, nil
}

func (d *CacheStore) syncOneZSet(key string, version string) (insert int, err error) {
	insert = 0
	needRemoveIfErr := true
	defer func() {
		if err != nil && needRemoveIfErr {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			syncKey := d.getSyncKeyZSet()
			_, err2 := d.options.cache.Eval(ctx, syncCacheScriptByScore, []string{syncKey}, key, version).Int()
			if err2 != nil {
				msg := fmt.Sprintf("syncOne fail to remove fail name:%v key:%v err:%v err2:%v", d.options.uniqName, key, err, err2)
				logrus.Error(msg)
			} else {
				msg := fmt.Sprintf("syncOne fail to remove succ name:%v key:%v err:%v", d.options.uniqName, key, err)
				logrus.Error(msg)
			}
		}
	}()
	rst, err := d.getByKeyForSync(key)
	if err != nil {
		return
	}
	//--------------------------------------------------------------------------------
	//从指针转成结构体类型
	if _, ok := reflect.TypeOf(rst).Elem().FieldByName(d.options.tdataIdFieldName); !ok {
		msg := fmt.Sprintf("syncOne fail no id field :%v", key)
		logrus.Error(msg)
		err = errors.New(msg)
		return
	}
	empty := reflect.Value{}
	//从指针转成结构体类型
	val := reflect.ValueOf(rst).Elem().FieldByName(d.options.tdataIdFieldName)
	if val == empty {
		msg := fmt.Sprintf("syncOne fail id field empty value:%v", key)
		logrus.Error(msg)
		err = errors.New(msg)
		return
	}
	//--------------------------------------------------------------------------------
	filter := bson.M{"_id": val.Interface()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	tableName := d.getTableName(val.Interface())
	coll := d.options.mongo.Database(d.options.mongoCfg.DB).Collection(tableName)
	err = coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": rst}).Err()
	if err != nil {
		_, err = coll.InsertOne(ctx, rst)
		if err != nil {
			msg := fmt.Sprintf("syncOne insert Err key:%v err:%v", key, err.Error())
			logrus.Error(msg)
			err = errors.New(msg)
			return
		}
		insert = 1
	}
	logrus.Infof("syncOne name=%v key=%v id=%v value:%v err:%v", d.options.uniqName, key, val.Interface(), rst, err)
	needRemoveIfErr = false
	syncKey := d.getSyncKeyZSet()
	_, err = d.options.cache.Eval(ctx, syncCacheScriptByScore, []string{syncKey}, key, version).Int()
	if err != nil {
		msg := fmt.Sprintf("syncOne fail to eval name:%v key:%v err:%v", d.options.uniqName, key, err)
		logrus.Error(msg)
		err = errors.New(msg)
		return
	}
	return
}

func (d *CacheStore) getSyncItems() (int64, int64, map[string]string, error) {
	return d.getSyncItemsZSet()
}

// func (d *CacheStore) getSyncKey() string {
// 	return d.getSyncKeyZSet()
// }
func (d *CacheStore) delSyncItem(key string) error {
	return d.delSyncItemZSet(key)
}
func (d *CacheStore) setSync(key string) error {
	return d.setSyncZSet(key)
}
func (d *CacheStore) syncOne(key string, version string) (int, error) {
	return d.syncOneZSet(key, version)
}
