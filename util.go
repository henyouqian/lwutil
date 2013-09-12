package lwutil

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"github.com/nu7hatch/gouuid"
	"net/http"
	"runtime"
	"time"
)

type Err struct {
	Error       string
	ErrorString string
}

func HandleError(w http.ResponseWriter) {
	if r := recover(); r != nil {
		w.WriteHeader(http.StatusBadRequest)
		encoder := json.NewEncoder(w)

		var err Err
		switch r.(type) {
		case Err:
			err = r.(Err)
		default:
			err = Err{"err_internal", fmt.Sprintf("%v", r)}

			// buf := make([]byte, 1024)
			// runtime.Stack(buf, false)
			// glog.Errorf("%v\n%s\n", r, buf)
		}

		buf := make([]byte, 1024)
		runtime.Stack(buf, false)
		glog.Errorf("%v\n%s\n", r, buf)

		encoder.Encode(&err)
	}
}

func PanicError(err error) {
	if err != nil {
		panic(err)
	}
}

func SendError(errType, errStr string) {
	panic(Err{errType, errStr})
}

func CheckError(err error, errType string) {
	if err != nil {
		if errType == "" {
			errType = "err_internal"
		}
		SendError(errType, fmt.Sprintf("%v", err))
	}
}

func CheckMathod(r *http.Request, method string) {
	if r.Method != method {
		SendError("err_method_not_allowed", "")
	}
}

func DecodeRequestBody(r *http.Request, v interface{}) {
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(v)
	CheckError(err, "err_decode_body")
}

func WriteResponse(w http.ResponseWriter, v interface{}) {
	encoder := json.NewEncoder(w)
	encoder.Encode(v)
}

func Sha224(s string) string {
	hasher := sha256.New224()
	hasher.Write([]byte(s))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func GenUUID() string {
	uuid, err := uuid.NewV4()
	CheckError(err, "")
	return base64.URLEncoding.EncodeToString(uuid[:])
}

func EndTx(tx *sql.Tx, err *error) {
	if *err == nil {
		tx.Commit()
	} else {
		tx.Rollback()
	}
}

func RepeatSingletonTask(redisPool redis.Pool, key string, interval time.Duration, f func()) {
	rc := redisPool.Get()
	defer rc.Close()

	intervalMin := 10 * time.Millisecond
	if interval < intervalMin {
		interval = intervalMin
	}

	fingerprint := GenUUID()
	redisKey := fmt.Sprintf("locker/%s", key)
	for {
		rdsfp, _ := redis.String(rc.Do("get", redisKey))
		if rdsfp == fingerprint {
			// it's mine
			_, err := rc.Do("expire", redisKey, int64(interval.Seconds())+1)
			CheckError(err, "")
			f()
			time.Sleep(interval)
			continue
		} else {
			// takeup
			if rdsfp == "" {
				_, err := rc.Do("setex", redisKey, int64(interval.Seconds())+1, fingerprint)
				CheckError(err, "")
				f()
				time.Sleep(interval)
				continue
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
