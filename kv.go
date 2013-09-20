package lwutil

import (
	"database/sql"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	"time"
)

const ()

var (
	kvDB      *sql.DB
	redisPool *redis.Pool
	cmdSetKV  *redis.Script
	cmdGetDel *redis.Script
)

const (
	CACHE_LIFE_SEC = 5
	SCRIPT_SET_KV  = `
		redis.call('set', 'kv/'..KEYS[1], KEYS[2])
		redis.call('zadd', 'kvz', KEYS[3], 'kv/'..KEYS[1])
	`
	SCRIPT_GETDEL = `
		local r = redis.call('ZRANGEBYSCORE', 'kvz', 0, KEYS[1], "LIMIT", 0, 100)
		redis.pcall('zrem', 'kvz', unpack(r))
		redis.pcall('del', unpack(r))
		return #(r)
	`
)

func StartKV(db *sql.DB, pool *redis.Pool) {
	kvDB = db
	redisPool = pool

	c := redisPool.Get()
	defer c.Close()

	//cmdSetKV: key, value, expireTime
	cmdSetKV = redis.NewScript(3, SCRIPT_SET_KV)
	err := cmdSetKV.Load(c)
	PanicIfError(err)

	//cmdGetDel: key, value, expireTime
	cmdGetDel = redis.NewScript(1, SCRIPT_GETDEL)
	err = cmdGetDel.Load(c)
	PanicIfError(err)

	//start save to db task
	go RepeatSingletonTask(redisPool, "kvSaveToDbTask", saveToDBTask)
}

func SetKV(key string, value []byte) error {
	rc := redisPool.Get()
	defer rc.Close()

	expireTime := GetRedisTime() + CACHE_LIFE_SEC

	err := cmdSetKV.SendHash(rc, key, value, expireTime)
	return NewErr(err)
}

func GetKV(key string) ([]byte, error) {
	rc := redisPool.Get()
	defer rc.Close()

	v, err := rc.Do("get", "kv/"+key)
	if v != nil {
		s, err := redis.Bytes(v, err)
		return s, NewErr(err)
	}

	//if cache miss
	// select from db
	row := kvDB.QueryRow("SELECT v FROM kvs WHERE k=?", key)
	var value []byte
	err = row.Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, NewErr(err)
		}
	}

	// write to redis
	expireTime := GetRedisTime() + CACHE_LIFE_SEC
	err = cmdSetKV.SendHash(rc, key, value, expireTime)

	//
	return value, NewErr(err)
}

func saveToDBTask() {
	glog.Infoln("saveToDBTask")

	rc := redisPool.Get()
	defer rc.Close()

	err := cmdGetDel.SendHash(rc, GetRedisTime())
	if err != nil {
		glog.Errorln(err)
	}
	rc.Flush()
	r, err := redis.Int(rc.Receive())
	glog.Infoln(r)
	if r != 0 {
		time.Sleep(time.Millisecond * 10)
	} else {
		time.Sleep(time.Second * 1)
	}
}
