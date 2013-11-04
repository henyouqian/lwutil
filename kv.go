package lwutil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	"reflect"
	"strings"
	"time"
)

const ()

var (
	kvDB            *DB
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

func StartKV(db *DB, pool *redis.Pool) {
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
	go RepeatSingletonTask(redisPool, "hkvSaveToDB", hkvSaveToDB)

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
	Db        *DB
	TableName string
	KeyName   string
	KeyValue  interface{}
	Value     interface{}
	Error     error
}

func hMakeKey(dbName, tableName, keyName string, keyValue interface{}) string {
	return fmt.Sprintf("hkv/%s %s %s %v", dbName, tableName, keyName, keyValue)
}

func HGetKvs(hkvs []Hkv) error {
	rc := redisPool.Get()
	defer rc.Close()

	//request
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

		key := hMakeKey(hkv.Db.Name, hkv.TableName, hkv.KeyName, hkv.KeyValue)
		args = append(args, key)

		vType := vValue.Type()
		for i := 0; i < numField; i++ {
			args = append(args, vType.Field(i).Name)
		}

		rc.Send("exists", key)
		rc.Send("hmget", args...)
	}
	err := rc.Flush()
	if err != nil {
		return NewErr(err)
	}

	//deal with reply
	needWriteToRedis := false
	for ihkv, hkv := range hkvs {
		existsInRedis, err := redis.Bool(rc.Receive())
		if err != nil {
			hkvs[ihkv].Error = err
			continue
		}

		vValue := reflect.ValueOf(hkv.Value)
		if vValue.Kind() == reflect.Ptr {
			vValue = vValue.Elem()
		}

		numField := vValue.NumField()
		args := make([]interface{}, 0, numField)
		for i := 0; i < numField; i++ {
			args = append(args, vValue.Field(i).Addr().Interface())
		}

		reply, err := redis.Values(rc.Receive())
		if err != nil {
			hkvs[ihkv].Error = err
			continue
		}
		redis.Scan(reply, args...)

		//need query form db?
		nilFieldNames := make([]string, 0, len(reply))
		nilFieldItfs := make([]interface{}, 0, len(reply))
		vType := vValue.Type()
		for i, r := range reply {
			if r == nil {
				nilFieldNames = append(nilFieldNames, vType.Field(i).Name)
				nilFieldItfs = append(nilFieldItfs, vValue.Field(i).Addr().Interface())
			}
		}

		if len(nilFieldNames) != 0 {
			strSql := fmt.Sprintf("SELECT %s FROM %s WHERE %s=%v",
				strings.Join(nilFieldNames, ","),
				hkv.TableName,
				hkv.KeyName,
				hkv.KeyValue)
			err := hkv.Db.QueryRow(strSql).Scan(nilFieldItfs...)
			if err != nil {
				hkvs[ihkv].Error = err
				continue
			} else { //if no error, then save to redis
				var args redis.Args
				key := hMakeKey(hkv.Db.Name, hkv.TableName, hkv.KeyName, hkv.KeyValue)
				args = args.Add(key)
				for i, v := range nilFieldNames {
					args = args.Add(v)
					args = args.Add(reflect.ValueOf(nilFieldItfs[i]).Elem().Interface())
				}
				err := rc.Send("hmset", args...)
				if err != nil {
					hkvs[ihkv].Error = err
					continue
				}
				needWriteToRedis = true

				//if not exists in redis before, then set expire time
				if !existsInRedis {
					rc.Send("expire", key, CACHE_LIFE_SEC)
				}
			}
		}
	}

	if needWriteToRedis {
		err = rc.Flush()
		return NewErr(err)
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
		args := make([]interface{}, 0, numField*2+1)

		key := hMakeKey(hkv.Db.Name, hkv.TableName, hkv.KeyName, hkv.KeyValue)
		args = append(args, key)

		vType := vValue.Type()
		for i := 0; i < numField; i++ {
			args = append(args, vType.Field(i).Name)
			args = append(args, vValue.Field(i).Interface())
		}

		rc.Send("hmset", args...)

		//hkvz
		t := GetRedisTimeUnix()
		rc.Send("zadd", "hkvz", t, key)
		rc.Send("persist", key)
	}

	err := rc.Flush()
	if err != nil {
		return NewErr(err)
	}

	hasError := false
	for i, _ := range hkvs {
		_, err = rc.Receive()
		if err != nil {
			hasError = true
		}
		hkvs[i].Error = err
	}

	if hasError {
		return NewErrStr("some error happend, please check error in hkvs")
	}
	return nil
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

func hkvSaveToDB() error {
	rc := redisPool.Get()
	defer rc.Close()
	glog.Errorln("hkvSaveToDB")

	// expireTime := GetRedisTimeUnix() + CACHE_LIFE_SEC
	expireTime := GetRedisTimeUnix()
	reply, err := redis.Strings(rc.Do("ZRANGEBYSCORE", "hkvz", 0, expireTime, "LIMIT", 0, 100))
	if err != nil {
		return NewErr(err)
	}

	if len(reply) != 0 {
		for _, key := range reply {
			rc.Send("hgetall", key)
		}
		err := rc.Flush()
		if err != nil {
			return NewErr(err)
		}
		for _, key := range reply {
			rpls, err := redis.Strings(rc.Receive())
			if err != nil {
				return NewErr(err)
			}
			var db, table, kn, kv string
			_, err = fmt.Sscanf(key, "hkv/%s%s%s%s", &db, &table, &kn, &kv)
			if err != nil {
				return NewErr(err)
			}
			sql := fmt.Sprintf("INSERT INTO %s SET %s=%s", table, kn, kv)
			for i := 0; i < len(rpls); {
				sql += fmt.Sprintf(", %s=%s", rpls[i], rpls[i+1])
				i += 2
			}
			glog.Errorln(sql, rpls)
		}
	}

	time.Sleep(time.Second * 1)

	return nil
}

func saveToDBTask() error {
	//glog.Infoln("saveToDBTask")

	rc := redisPool.Get()
	defer rc.Close()

	redisTime := GetRedisTimeUnix()

	//get expired
	err := cmdGetExpiredKV.SendHash(rc, redisTime)
	if err != nil {
		return err
	}
	rc.Flush()
	delobjs, err := redis.Values(rc.Receive())
	if err != nil {
		glog.Errorln(err)
		time.Sleep(time.Second * 1)
		return err
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
			return err
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

	return nil
}
