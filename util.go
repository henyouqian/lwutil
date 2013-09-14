package lwutil

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"github.com/nu7hatch/gouuid"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Err struct {
	Error       string
	ErrorString string
}

func handleError(w http.ResponseWriter) {
	if r := recover(); r != nil {
		encoder := json.NewEncoder(w)

		var err Err
		switch r.(type) {
		case Err:
			err = r.(Err)
		default:
			err = Err{"", fmt.Sprintf("%v", r)}
		}

		if err.Error == "" {
			err.Error = "err_internal"
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}

		//buf := make([]byte, 1024)
		//runtime.Stack(buf, false)
		//glog.Errorf("%v\n%s\n", r, buf)
		glog.Errorln(r)

		encoder.Encode(&err)
	}
}

type ReqHandler func(http.ResponseWriter, *http.Request)

func (fn ReqHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer handleError(w)
	fn(w, r)
}

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func CheckError(errType string, err error) {
	if err == nil {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	errStr := fmt.Sprintf("%s\n\t%s : %d", err.Error(), file, line)
	panic(Err{errType, errStr})
}

func SendError(errType, errStr string) {
	_, file, line, _ := runtime.Caller(1)
	errStr = fmt.Sprintf("%s\n\t%s : %d", errStr, file, line)
	panic(Err{errType, errStr})
}

//func CheckError(err error, errType string) {
//	if err != nil {
//		if errType == "" {
//			errType = "err_internal"
//		}
//		SendError(errType, fmt.Sprintf("%v", err))
//	}
//}

func CheckMathod(r *http.Request, method string) {
	if r.Method != method {
		SendError("err_method_not_allowed", "")
	}
}

func DecodeRequestBody(r *http.Request, v interface{}) {
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(v)
	CheckError("err_decode_body", err)
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
	CheckError("", err)
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
			CheckError("", err)
			f()
			time.Sleep(interval)
			continue
		} else {
			// takeup
			if rdsfp == "" {
				_, err := rc.Do("setex", redisKey, int64(interval.Seconds())+1, fingerprint)
				CheckError("", err)
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

func Truncate(v, min, max int64) int64 {
	if v > max {
		return max
	}
	if v < min {
		return min
	}
	return v
}

func LoadCsvTbl(file string, keycols []string, tbl interface{}) (err error) {
	f, err := os.Open(file)
	if err != nil {
		return NewErr(err)
	}
	defer f.Close()

	//
	v := reflect.ValueOf(tbl).Elem()
	defer func() {
		if r := recover(); r != nil {
			err = NewErrStr(fmt.Sprintf("tbl's type must be map[string]struct. detail:%v", r))
		}
	}()

	t := v.Type()
	if v.IsNil() {
		v.Set(reflect.MakeMap(t))
	}
	rowObjType := t.Elem()

	//
	reader := csv.NewReader(f)
	firstrow, err := reader.Read()
	keycolidxs := make([]int, len(keycols))
	for icol, vcol := range keycols {
		found := false
		for i, v := range firstrow {
			if strings.EqualFold(v, vcol) {
				keycolidxs[icol] = i
				found = true
				break
			}
		}
		if !found {
			return NewErrStr(fmt.Sprintf("column not found: %s in %s", vcol, file))
		}
	}

	if len(keycolidxs) != len(keycols) {
		return NewErrStr(fmt.Sprintf("keys not match totally: keycols = %v", keycols))
	}

	row, err := reader.Read()
	for row != nil {
		rowobjValue := reflect.New(rowObjType).Elem()
		numField := rowobjValue.NumField()
		for i := 0; i < numField; i++ {
			f := rowobjValue.Field(i)
			colname := rowobjValue.Type().Field(i).Name

			colidx := -1
			for i, v := range firstrow {
				if strings.EqualFold(colname, v) {
					colidx = i
					break
				}
			}
			if colidx != -1 {
				valstr := row[colidx]
				switch f.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					n, err := strconv.ParseInt(valstr, 0, 64)
					if err != nil {
						return NewErr(err)
					}
					f.SetInt(n)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					n, err := strconv.ParseUint(valstr, 0, 64)
					if err != nil {
						return NewErr(err)
					}
					f.SetUint(n)
				case reflect.Float32, reflect.Float64:
					n, err := strconv.ParseFloat(valstr, 64)
					if err != nil {
						return NewErr(err)
					}
					f.SetFloat(n)
				case reflect.Bool:
					n, err := strconv.ParseBool(valstr)
					if err != nil {
						return NewErr(err)
					}
					f.SetBool(n)
				case reflect.String:
					f.SetString(valstr)
				}
			}
		}

		keys := make([]string, len(keycolidxs))
		for i, v := range keycolidxs {
			keys[i] = row[v]
		}
		v.SetMapIndex(reflect.ValueOf(strings.Join(keys, ",")), rowobjValue)

		row, err = reader.Read()
	}

	return nil
}

func GetStructFieldKVs(data interface{}) ([]string, []interface{}, error) {
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Struct {
		return nil, nil, NewErrStr("arg data must be struct")
	}

	numField := value.NumField()
	names := make([]string, numField)
	vals := make([]interface{}, numField)
	for i := 0; i < numField; i++ {
		field := value.Field(i)
		name := value.Type().Field(i).Name
		names[i] = name
		if field.CanInterface() {
			vals[i] = field.Interface()
		} else {
			return nil, nil, NewErrStr("encount unexport field")
		}
	}

	return names, vals, nil
}

func GetStructFieldKeys(data interface{}) ([]string, error) {
	value := reflect.ValueOf(data)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, NewErrStr("arg data must be struct or struct pointer")
	}

	numField := value.NumField()
	names := make([]string, numField)
	for i := 0; i < numField; i++ {
		name := value.Type().Field(i).Name
		names[i] = name
	}

	return names, nil
}

func GetStructFieldValues(data interface{}) ([]interface{}, error) {
	value := reflect.ValueOf(data)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, NewErrStr("arg data must be struct or struct pointer")
	}

	numField := value.NumField()
	vals := make([]interface{}, numField)
	for i := 0; i < numField; i++ {
		field := value.Field(i)
		if field.CanInterface() {
			vals[i] = field.Interface()
		} else {
			return nil, NewErrStr("encount unexport field")
		}
	}

	return vals, nil
}

func NewErr(err error) error {
	_, file, line, _ := runtime.Caller(1)
	return errors.New(fmt.Sprintf("%s\n\t%s : %d", err.Error(), file, line))
}

func NewErrStr(err string) error {
	_, file, line, _ := runtime.Caller(1)
	return errors.New(fmt.Sprintf("%s\n\t%s : %d", err, file, line))
}
