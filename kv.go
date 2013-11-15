package lwutil

import (
	"encoding/json"
	//"fmt"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/henyouqian/lvdb"
	//"github.com/golang/glog"
	//"strings"
)

var (
	kvRedisPool *redis.Pool
	lvdbPool    *lvDB.Pool
)

func KvStart(pool *redis.Pool) error {
	kvRedisPool = pool
	lvdbPool = lvDB.NewPool("127.0.0.1:1234", 10)
	return nil
}

type Kv struct {
	Key   interface{}
	Value interface{}
}

func kvMakeLvdbKey(keyRaw interface{}) ([]byte, error) {
	r := []byte(fmt.Sprintf("%v", keyRaw))
	if len(r) == 0 {
		return r, NewErrStr("empty key")
	}
	return r, nil
}

func kvMakeRedisKey(keyRaw interface{}) ([]byte, error) {
	r := []byte(fmt.Sprintf("_kv/%v", keyRaw))
	if len(r) == 0 {
		return r, NewErrStr("empty key")
	}
	return r, nil
}

func KvPut(kvs ...Kv) error {
	rc := kvRedisPool.Get()
	defer rc.Close()

	for _, kv := range kvs {
		redisKey, err := kvMakeRedisKey(kv.Key)
		if err != nil {
			return NewErr(err)
		}

		bytes, err := json.Marshal(kv.Value)
		if err != nil {
			return NewErr(err)
		}
		rc.Send("set", redisKey, bytes)
		rc.Send("zadd", "_kvz", GetRedisTimeUnix(), redisKey)
	}

	err := rc.Flush()
	return NewErr(err)
}

func KvGet(keys ...interface{}) ([][]byte, error) {
	keyNum := len(keys)
	replies := make([][]byte, keyNum)
	exists := make([]bool, keyNum)
	lvdbKeys := make([][]byte, 0, keyNum)

	rc := kvRedisPool.Get()
	defer rc.Close()

	//redis
	for i, key := range keys {
		redisKey, err := kvMakeRedisKey(key)
		if err != nil {
			return nil, NewErr(err)
		}

		replies[i], err = redis.Bytes(rc.Do("get", redisKey))
		if err == redis.ErrNil {
			lvdbKey, err := kvMakeLvdbKey(key)
			if err != nil {
				return nil, NewErr(err)
			}
			lvdbKeys = append(lvdbKeys, lvdbKey)
			exists[i] = false
		} else if err != nil {
			return nil, NewErr(err)
		} else {
			exists[i] = true
		}
	}

	if len(lvdbKeys) == 0 {
		return replies, nil
	}

	//lvdb
	client, err := lvdbPool.Get()
	if err != nil {
		return nil, NewErr(err)
	}
	defer client.Close()

	dbValues, err := client.Get(lvdbKeys...)
	if err != nil {
		return nil, NewErr(err)
	}

	if len(lvdbKeys) != len(dbValues) {
		return nil, NewErrStr("len(lvdbKeys) != len(dbValues)")
	}

	idbv := 0
	for i, _ := range replies {
		if exists[i] == false {
			replies[i] = dbValues[idbv]
			idbv++
		}
	}

	return replies, NewErr(err)
}

func KvPutDb(kvs ...Kv) error {
	client, err := lvdbPool.Get()
	if err != nil {
		return NewErr(err)
	}
	defer client.Close()

	lvdbKvs := make([]lvDB.Kv, len(kvs))
	for i, kv := range kvs {
		lvdbKvs[i].Key, err = kvMakeLvdbKey(kv.Key)
		if err != nil {
			return NewErr(err)
		}
		lvdbKvs[i].Value, err = json.Marshal(kv.Value)
		if err != nil {
			return NewErr(err)
		}
	}

	err = client.Put(lvdbKvs...)

	return NewErr(err)
}

func KvGetDb(keys ...interface{}) ([][]byte, error) {
	client, err := lvdbPool.Get()
	if err != nil {
		return nil, NewErr(err)
	}
	defer client.Close()

	lvdbKeys := make([][]byte, len(keys))
	for i, key := range keys {
		lvdbKeys[i], err = kvMakeLvdbKey(key)
		if err != nil {
			return nil, NewErr(err)
		}
	}

	replies, err := client.Get(lvdbKeys...)

	if len(replies) != len(keys) {
		return nil, NewErrStr("len(replies) != len(kvs)")
	}
	return replies, NewErr(err)
}

func KvDelDb(keys ...interface{}) error {
	client, err := lvdbPool.Get()
	if err != nil {
		return NewErr(err)
	}
	defer client.Close()

	lvdbKeys := make([][]byte, len(keys))
	for i, key := range keys {
		lvdbKeys[i], err = kvMakeLvdbKey(key)
		if err != nil {
			return NewErr(err)
		}
	}

	err = client.Del(lvdbKeys...)
	return NewErr(err)
}

func KvScan(in [][]byte, out ...interface{}) error {
	if len(out) > len(in) {
		return NewErrStr(fmt.Sprintf("len(out) > len(in): out=%v, in=%v", out, in))
	}
	for i, _ := range out {
		err := json.Unmarshal(in[i], out[i])
		if err != nil {
			return NewErr(err)
		}
	}
	return nil
}
