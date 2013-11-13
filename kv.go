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

func KvStart(pool *redis.Pool) {
	kvRedisPool = pool
	lvdbPool = lvDB.NewPool("127.0.0.1:1234", 10)
}

type Kv struct {
	Key   interface{}
	Value interface{}
}

func kvMakeKey(keyRaw interface{}) ([]byte, error) {
	r := []byte(fmt.Sprintf("%v", keyRaw))
	if len(r) == 0 {
		return r, NewErrStr("empty key")
	}
	return r, nil
}

func KvPut(kvs ...Kv) error {
	client, err := lvdbPool.Get()
	if err != nil {
		return NewErr(err)
	}
	defer client.Close()

	lvdbKvs := make([]lvDB.Kv, len(kvs))
	for i, kv := range kvs {
		lvdbKvs[i].Key, err = kvMakeKey(kv.Key)
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

func KvGet(keys ...interface{}) ([][]byte, error) {
	client, err := lvdbPool.Get()
	if err != nil {
		return nil, NewErr(err)
	}
	defer client.Close()

	lvdbKeys := make([][]byte, len(keys))
	for i, key := range keys {
		lvdbKeys[i], err = kvMakeKey(key)
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

func KvScan(in [][]byte, out ...interface{}) error {
	if len(out) > len(in) {
		return NewErrStr("len(out) > len(in)")
	}
	for i, _ := range out {
		err := json.Unmarshal(in[i], out[i])
		if err != nil {
			return NewErr(err)
		}
	}
	return nil
}

func KvDel(key interface{}) error {
	return nil
}
