package lwutil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	"reflect"
	"time"
)

const ()

var (
	kvDB            *sql.DB
	redisPool       *redis.Pool
	cmdSetKV        *redis.Script
	cmdGetDel       *redis.Script
	cmdGetExpiredKV *redis.Script
)

const (
	CACHE_LIFE_SEC = 3600
	SCRIPT_SET_KV  = `
		redis.call('set', 'kv/'..KEYS[1], KEYS[2])
		redis.call('zadd', 'kvz', KEYS[3], 'kv/'..KEYS[1])
	`
	SCRIPT_GET_EXPIRED_KV = `
		local r1 = redis.pcall('ZRANGEBYSCORE', 'kvz', 0, KEYS[1], "LIMIT", 0, 100)
		local r2 = redis.pcall('mget', unpack(r1))
		local r = {}
		for i, v in ipairs(r1) do
			table.insert(r, r1[i])
			table.insert(r, r2[i])
		end
		return r
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

	//cmdGetExpiredKV: key, value, expireTime
	cmdGetExpiredKV = redis.NewScript(1, SCRIPT_GET_EXPIRED_KV)
	err = cmdGetExpiredKV.Load(c)
	PanicIfError(err)

	//start save to db task
	go RepeatSingletonTask(redisPool, "kvSaveToDbTask", saveToDBTask)
}

func SetKv(key string, value interface{}, rc redis.Conn) error {
	if rc == nil {
		rc = redisPool.Get()
		defer rc.Close()
	}
	expireTime := GetRedisTimeUnix() + CACHE_LIFE_SEC

	bt, err := json.Marshal(&value)
	if err != nil {
		return NewErr(err)
	}

	err = cmdSetKV.SendHash(rc, key, bt, expireTime)
	return NewErr(err)
}

func GetKv(key string, out interface{}, rc redis.Conn) (exist bool, err error) {
	if rc == nil {
		rc = redisPool.Get()
		defer rc.Close()
	}

	v, err := rc.Do("get", "kv/"+key)
	if err != nil {
		return false, NewErr(err)
	}

	expireTime := GetRedisTimeUnix() + CACHE_LIFE_SEC
	var bytes []byte
	if v != nil {
		bytes, err = redis.Bytes(v, err)
		if err != nil {
			return false, NewErr(err)
		}

		//update ttl
		_, err = rc.Do("zadd", "kvz", expireTime, "kv/"+key)
		if err != nil {
			return false, NewErr(err)
		}

	} else {
		//if cache miss, select from db
		row := kvDB.QueryRow("SELECT v FROM kvs WHERE k=?", key)
		err = row.Scan(&bytes)
		if err != nil {
			if err == sql.ErrNoRows {
				return false, nil
			} else {
				return false, NewErr(err)
			}
		}
		//write to redis
		err = cmdSetKV.SendHash(rc, key, bytes, expireTime)
	}

	//out
	err = json.Unmarshal(bytes, out)
	return err == nil, NewErr(err)
}

func SetKvDb(key string, value interface{}) error {
	bts, err := json.Marshal(&value)
	if err != nil {
		return NewErr(err)
	}
	_, err = kvDB.Exec("REPLACE INTO kvs (k, v) VALUES(?, ?)", key, bts)
	return NewErr(err)
}

func GetKvDb(key string, out interface{}) (exist bool, err error) {
	row := kvDB.QueryRow("SELECT v FROM kvs WHERE k=?", key)
	var bytes []byte
	err = row.Scan(&bytes)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		} else {
			return false, NewErr(err)
		}
	}
	err = json.Unmarshal(bytes, out)
	return err == nil, NewErr(err)
}

type Hkv struct {
	TableName string
	KeyName   string
	KeyValue  interface{}
	Value     interface{}
	Error     error
}

func HGetKvs(hkvs []Hkv) error {
	rc := redisPool.Get()
	defer rc.Close()

	for _, hkv := range hkvs {
		vValue := reflect.ValueOf(hkv.Value)
		if vValue.Kind() == reflect.Ptr {
			vValue = vValue.Elem()
		}
		if vValue.Kind() != reflect.Struct {
			return NewErrStr("err_not_struct")
		}

		numField := vValue.NumField()
		args := make([]interface{}, 0, numField+1)

		key := fmt.Sprintf("hkv/%s/%v", hkv.TableName, hkv.KeyValue)
		args = append(args, key)

		vType := vValue.Type()
		for i := 0; i < numField; i++ {
			args = append(args, vType.Field(i).Name)
		}

		rc.Send("hmget", args...)
	}
	err := rc.Flush()
	if err != nil {
		return NewErr(err)
	}

	for _, hkv := range hkvs {
		_ = hkv
	}

	return nil
}

func HSetKvs(hkvs []Hkv) error {
	rc := redisPool.Get()
	defer rc.Close()

	for _, hkv := range hkvs {
		vValue := reflect.ValueOf(hkv.Value)
		if vValue.Kind() == reflect.Ptr {
			vValue = vValue.Elem()
		}
		if vValue.Kind() != reflect.Struct {
			return NewErrStr("err_not_struct")
		}

		numField := vValue.NumField()
		args := make([]interface{}, 0, numField*2+5)

		key := fmt.Sprintf("hkv/%s/%v", hkv.TableName, hkv.KeyValue)
		args = append(args, key)
		args = append(args, "_kn")
		args = append(args, hkv.KeyName)
		args = append(args, "_kv")
		args = append(args, fmt.Sprintf("%v", hkv.KeyValue))

		vType := vValue.Type()
		for i := 0; i < numField; i++ {
			args = append(args, vType.Field(i).Name)
			args = append(args, vValue.Field(i).Interface())
		}

		err := rc.Send("hmset", args...)
		if err != nil {
			return NewErr(err)
		}
	}

	err := rc.Flush()
	return NewErr(err)
}

func hSetKv(db *sql.DB, tableName string, keyName string, keyValue interface{}, value interface{}) error {
	vValue := reflect.ValueOf(value)
	if vValue.Kind() == reflect.Ptr {
		vValue = vValue.Elem()
	}
	if vValue.Kind() != reflect.Struct {
		return NewErrStr("err_not_struct")
	}

	rc := redisPool.Get()
	defer rc.Close()

	numField := vValue.NumField()
	args := make([]interface{}, 0, numField*2+3)

	key := fmt.Sprintf("hkv/%s/%v", tableName, keyValue)
	args = append(args, key)
	args = append(args, "_k")
	args = append(args, fmt.Sprintf("%s=%v", keyName, keyValue))

	vType := vValue.Type()
	for i := 0; i < numField; i++ {
		args = append(args, vType.Field(i).Name)
		args = append(args, vValue.Field(i).Interface())
	}

	_, err := rc.Do("hmset", args...)
	return NewErr(err)
}

func hGetKv(db *sql.DB, tableName string, keyName string, keyValue interface{}, value interface{}) error {

	return nil
}

//func DelKV(key string) error {
//	_, err := kvDB.Exec("DELETE FROM kvs WHERE k=?", key)
//	return NewErr(err)
//}

func saveToDBTask() {
	//glog.Infoln("saveToDBTask")

	rc := redisPool.Get()
	defer rc.Close()

	redisTime := GetRedisTimeUnix()

	//get expired
	err := cmdGetExpiredKV.SendHash(rc, redisTime)
	if err != nil {
		glog.Errorln(err)
		time.Sleep(time.Second * 1)
		return
	}
	rc.Flush()
	delobjs, err := redis.Values(rc.Receive())
	if err != nil {
		glog.Errorln(err)
		time.Sleep(time.Second * 1)
		return
	}

	if len(delobjs) != 0 {
		//save to db
		err := func() error {
			//setup transaction
			tx, err := kvDB.Begin()
			if err != nil {
				return NewErr(err)
			}
			defer EndTx(tx, &err)

			stmt, err := tx.Prepare(`REPLACE INTO kvs
	            (k, v)
				VALUES(?, ?)`)
			if err != nil {
				return NewErr(err)
			}

			for i := 0; i < len(delobjs)/2; i++ {
				k := delobjs[i*2]
				k = k.([]uint8)[3:]
				v := delobjs[i*2+1]
				_, err := stmt.Exec(k, v)
				if err != nil {
					return NewErr(err)
				}
			}
			return nil
		}()
		if err != nil {
			glog.Errorln(err)
			time.Sleep(time.Second * 1)
			return
		}

		//del redis data
		err = cmdGetDel.SendHash(rc, redisTime)
		if err != nil {
			glog.Errorln(err)
		}
		rc.Flush()
		time.Sleep(time.Millisecond * 10)
	} else {
		time.Sleep(time.Second * 1)
	}
}
