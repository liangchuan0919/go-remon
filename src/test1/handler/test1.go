package handler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/tideland/golib/logger"
	test1config "go-remon.lc/src/test1/test1config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/sirupsen/logrus"
)

type Test1Handler struct {
	cache *redis.Client
	mongo *mongo.Client
	store *Test1Store
}

func InitMongo() *mongo.Client {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongourl := test1config.MONGOURL
	//连接池
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongourl).SetMaxPoolSize(20))
	if err != nil {
		logrus.Errorf("mongo.Connect------: %v %v ", err, mongourl)
		return nil
	}

	return client
}

func InitCache() *redis.Client {
	cachehost := test1config.REDISCACHEHOST
	cacheport := test1config.REDISCACHEPORT
	cacheauth := test1config.REDISCACHEAUTH
	cachedb := test1config.REDISCACHEDB

	cache := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cachehost, cacheport),
		Password: cacheauth,
		DB:       cachedb,
	})

	return cache
}

func (h *Test1Handler) Init() error {
	h.cache = InitCache()
	if h.cache == nil {
		err := errors.New("InitCache error")
		logrus.Errorf("InitCache------: %v ", err)
		return err
	}

	h.mongo = InitMongo()
	if h.mongo == nil {
		err := errors.New("InitMongo error")
		logrus.Errorf("InitMongo------: %v ", err)
		return err
	}
	err := h.initStore()
	if err != nil {
		logger.Errorf("h.initStore------: %v ", err)
		return err
	}
	return nil
}
