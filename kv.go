package lwutil

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
)

var (
	kvRedisPool *redis.Pool
)

func KvStart(pool *redis.Pool) {
	kvRedisPool = pool
}

type KvData struct {
	Db    *DB
	Table string
	Key   interface{}
	Value interface{}
	Error error
}

func kvMakeKey(data *KvData) (dbKey, redisKey string) {
	dbKey = fmt.Sprintf("%v", data.Key)
	if len(dbKey) > 40 {
		dbKey = Sha224(dbKey)
	}
	redisKey = fmt.Sprintf("kv/%s/%s/%s", data.Db.Name, data.Table, dbKey)
	return dbKey, redisKey
}

func KvGet(kvs []KvData) error {
	if len(kvs) == 0 {
		return nil
	}

	rc := kvRedisPool.Get()
	defer rc.Close()

	redisKeys := make([]string, len(kvs))
	dbKeys := make([]string, len(kvs))

	//try redis first
	for i, data := range kvs {
		dbKey, redisKey := kvMakeKey(&data)
		redisKeys[i] = redisKey
		dbKeys[i] = dbKey
		rc.Send("get", redisKey)
	}

	err := rc.Flush()
	if err != nil {
		return NewErr(err)
	}

	needFlush := false
	for i, data := range kvs {
		bytes, err := redis.Bytes(rc.Receive())
		if err != nil && err != redis.ErrNil {
			kvs[i].Error = err
		}
		if len(bytes) == 0 { //redis miss, try db
			row := data.Db.QueryRow("SELECT v FROM test WHERE k=?", dbKeys[i])
			err = row.Scan(&bytes)
			if err != nil {
				kvs[i].Error = err
				continue
			}

			err := json.Unmarshal(bytes, kvs[i].Value)
			if err != nil {
				kvs[i].Error = err
				continue
			}

			//write to redis
			needFlush = true
			rc.Send("setex", redisKeys[i], CACHE_LIFE_SEC, bytes)
		} else { //redis hit
			err := json.Unmarshal(bytes, kvs[i].Value)
			if err != nil {
				kvs[i].Error = err
				continue
			}
		}
	}

	if needFlush {
		err := rc.Flush()
		if err != nil {
			return NewErr(err)
		}
	}

	return nil
}

func KvSet(kvs []KvData) error {
	if len(kvs) == 0 {
		return nil
	}

	rc := kvRedisPool.Get()
	defer rc.Close()

	for i, data := range kvs {
		_, redisKey := kvMakeKey(&data)

		bytes, err := json.Marshal(data.Value)
		if err != nil {
			kvs[i].Error = err
			continue
		}
		rc.Send("set", redisKey, bytes)
		rc.Send("zadd", "kvz", GetRedisTimeUnix(), redisKey)
	}

	err := rc.Flush()
	if err != nil {
		return NewErr(err)
	}

	return nil
}

func KvSaveTask() error {
	rc := kvRedisPool.Get()
	defer rc.Close()
	//glog.Errorln("hkvSaveToDB")

	// expireTime := GetRedisTimeUnix() + CACHE_LIFE_SEC
	expireTime := GetRedisTimeUnix()
	keys, err := redis.Values(rc.Do("ZRANGEBYSCORE", "kvz", 0, expireTime, "LIMIT", 0, 100))
	if err != nil {
		return NewErr(err)
	}

	if len(keys) != 0 {
		values, err := redis.Values(rc.Do("mget", keys...))

		if err != nil {
			return NewErr(err)
		}
		for i, key := range keys {
			glog.Errorln(key, values[i])
		}
	}

	return nil
}
