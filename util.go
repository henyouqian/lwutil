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
	"io"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	timeDiff                    int64
	HttpCodeInternalServerError = http.StatusInternalServerError
	HttpCodeBadRequest          = http.StatusBadRequest

	DbMap = make(map[string]*DB)
)

type Err struct {
	Error       string
	ErrorString string
}

type DB struct {
	*sql.DB
	Name string
}

func OpenDb(dbname string) (*DB, error) {
	dbRaw, err := sql.Open("mysql", fmt.Sprintf("root@/%s?parseTime=false", dbname))
	if err != nil {
		return nil, NewErr(err)
	}

	db := DB{dbRaw, dbname}
	DbMap[dbname] = &db

	return &db, nil
}

func init() {
	c, err := redis.Dial("tcp", "localhost:6379")
	PanicIfError(err)
	defer c.Close()

	t, err := redis.Values(c.Do("time"))
	PanicIfError(err)
	var sec int64
	_, err = redis.Scan(t, &sec)
	PanicIfError(err)

	timeDiff = sec - time.Now().Unix()
}

func GetRedisTime() time.Time {
	return time.Now().Add(time.Duration(timeDiff) * time.Second)
}

func GetRedisTimeUnix() int64 {
	return time.Now().Unix() + timeDiff
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
			buf := make([]byte, 2048)
			runtime.Stack(buf, false)
			glog.Errorf("%v\n%s\n", r, buf)
		}

		if err.Error == "" {
			err.Error = "err_internal"
			w.WriteHeader(HttpCodeInternalServerError)
		} else {
			w.WriteHeader(HttpCodeBadRequest)
		}

		glog.Errorln(r)

		encoder.Encode(&err)
	}
}

type ReqHandler func(http.ResponseWriter, *http.Request)

func (fn ReqHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer handleError(w)
	fn(w, r)
}

func HttpTest(handler ReqHandler, w http.ResponseWriter, req *http.Request) {
	ReqHandler(handler).ServeHTTP(w, req)
}

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func CheckError(err error, errType string) {
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

func DecodeRequestBody(r *http.Request, v interface{}) error {
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(v)
	return err
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

func Sha256(s string) string {
	hasher := sha256.New()
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

//use go keyword to start a goroutine
func RepeatSingletonTask(redisPool *redis.Pool, key string, f func()) {
	rc := redisPool.Get()
	defer rc.Close()

	fingerprint := GenUUID()
	redisKey := fmt.Sprintf("locker/%s", key)
	for {
		rdsfp, _ := redis.String(rc.Do("get", redisKey))
		if rdsfp == fingerprint {
			// it's mine
			_, err := rc.Do("expire", redisKey, 2)
			CheckError(err, "")
			f()
			runtime.Gosched()
			continue
		} else {
			// takeup
			if rdsfp == "" {
				_, err := rc.Do("setex", redisKey, 2, fingerprint)
				CheckError(err, "")
				f()
				runtime.Gosched()
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

//use "key1,key2" for multi column key
//mapPtr must be *map[string]struct
func LoadCsvMap(file string, keyCols []string, mapPtr interface{}) (err error) {
	f, err := os.Open(file)
	if err != nil {
		return NewErr(err)
	}
	defer f.Close()

	//check mapPtr's type
	typeOk := false
	mapPtrType := reflect.TypeOf(mapPtr)
	var structType reflect.Type
	if mapPtrType.Kind() == reflect.Ptr {
		t := mapPtrType.Elem()
		if t.Kind() == reflect.Map {
			kt := t.Key()
			structType = t.Elem()
			if kt.Kind() == reflect.String && structType.Kind() == reflect.Struct {
				typeOk = true
			}
		}
	}
	if !typeOk {
		return NewErrStr(fmt.Sprintf("input type must be ptr of map[string]struct. inputType=%s", mapPtrType.Name()))
	}

	//if map is nil, make map
	mapValue := reflect.ValueOf(mapPtr).Elem()
	if mapValue.IsNil() {
		mapValue.Set(reflect.MakeMap(mapValue.Type()))
	}

	//read csv header
	reader := csv.NewReader(f)
	firstRow, err := reader.Read()
	if err != nil {
		return NewErr(err)
	}

	// key column index
	keyColIdxs := make([]int, len(keyCols))
	for icol, vcol := range keyCols {
		found := false
		for i, v := range firstRow {
			if strings.EqualFold(v, vcol) {
				keyColIdxs[icol] = i
				found = true
				break
			}
		}
		if !found {
			return NewErrStr(fmt.Sprintf("key column not found: %s in %s", vcol, file))
		}
	}
	if len(keyColIdxs) != len(keyCols) {
		return NewErrStr(fmt.Sprintf("keys not match totally: keyCols = %v", keyCols))
	}

	// struct field column index
	numField := structType.NumField()
	fieldColIdxs := make([]int, numField)
	for iField := 0; iField < numField; iField++ {
		field := structType.Field(iField)
		colName := field.Tag.Get("csv")
		if colName == "" {
			colName = field.Name
		}

		found := false
		for i, v := range firstRow {
			if strings.EqualFold(v, colName) {
				fieldColIdxs[iField] = i
				found = true
				break
			}
		}
		if !found {
			return NewErrStr(fmt.Sprintf("field column not found: %s in %s", colName, file))
		}
	}

	//fill the map
	rowStrs, err := reader.Read()
	if err != nil {
		return NewErrStr(fmt.Sprintf("file=%s error=%s", file, err.Error()))
	}
	iRow := 1
	for rowStrs != nil {
		iRow++
		structValue := reflect.New(structType).Elem()
		for i := 0; i < numField; i++ {
			colIdx := fieldColIdxs[i]
			valstr := rowStrs[colIdx]
			fieldValue := structValue.Field(i)

			switch fieldValue.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				n, err := strconv.ParseInt(valstr, 0, 64)
				if err != nil {
					return NewErrStr(fmt.Sprintf("file=%s, row=%d, col=%s, error=%s", file, iRow, firstRow[colIdx], err.Error()))
				}
				fieldValue.SetInt(n)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				n, err := strconv.ParseUint(valstr, 0, 64)
				if err != nil {
					return NewErrStr(fmt.Sprintf("file=%s, row=%d, col=%s, error=%s", file, iRow, firstRow[colIdx], err.Error()))
				}
				fieldValue.SetUint(n)
			case reflect.Float32, reflect.Float64:
				n, err := strconv.ParseFloat(valstr, 64)
				if err != nil {
					return NewErrStr(fmt.Sprintf("file=%s, row=%d, col=%s, error=%s", file, iRow, firstRow[colIdx], err.Error()))
				}
				fieldValue.SetFloat(n)
			case reflect.Bool:
				n, err := strconv.ParseBool(valstr)
				if err != nil {
					return NewErrStr(fmt.Sprintf("file=%s, row=%d, col=%s, error=%s", file, iRow, firstRow[colIdx], err.Error()))
				}
				fieldValue.SetBool(n)
			case reflect.String:
				fieldValue.SetString(valstr)
			}
		}

		keys := make([]string, len(keyColIdxs))
		for i, v := range keyColIdxs {
			keys[i] = rowStrs[v]
		}
		mapValue.SetMapIndex(reflect.ValueOf(strings.Join(keys, ",")), structValue)

		rowStrs, err = reader.Read()
		if err != nil && err != io.EOF {
			return NewErrStr(fmt.Sprintf("file=%s, row=%d, error=%s", file, iRow, err.Error()))
		}
	}

	return nil
}

////use "key1,key2" for multi column key
//func LoadCsvTbl(file string, keycols []string, tbl interface{}) (err error) {
//	f, err := os.Open(file)
//	if err != nil {
//		return NewErr(err)
//	}
//	defer f.Close()

//	//
//	v := reflect.ValueOf(tbl).Elem()
//	defer func() {
//		if r := recover(); r != nil {
//			err = NewErrStr(fmt.Sprintf("tbl's type must be map[string]struct. detail:%v", r))
//		}
//	}()

//	t := v.Type()
//	if v.IsNil() {
//		v.Set(reflect.MakeMap(t))
//	}
//	rowObjType := t.Elem()

//	//
//	reader := csv.NewReader(f)
//	firstrow, err := reader.Read()
//	keycolidxs := make([]int, len(keycols))
//	for icol, vcol := range keycols {
//		found := false
//		for i, v := range firstrow {
//			if strings.EqualFold(v, vcol) {
//				keycolidxs[icol] = i
//				found = true
//				break
//			}
//		}
//		if !found {
//			return NewErrStr(fmt.Sprintf("column not found: %s in %s", vcol, file))
//		}
//	}

//	if len(keycolidxs) != len(keycols) {
//		return NewErrStr(fmt.Sprintf("keys not match totally: keycols = %v", keycols))
//	}

//	row, err := reader.Read()
//	iRow := 1
//	for row != nil {
//		iRow++
//		rowobjValue := reflect.New(rowObjType).Elem()
//		numField := rowObjType.NumField()
//		for i := 0; i < numField; i++ {
//			f := rowobjValue.Field(i)
//			colname := rowObjType.Field(i).Name
//			colidx := -1
//			for i, v := range firstrow {
//				if strings.EqualFold(colname, v) {
//					colidx = i
//					break
//				}
//			}
//			if colidx != -1 {
//				valstr := row[colidx]
//				if valstr == "" {
//					return NewErrStr(fmt.Sprintf("empty field: row=%d, field=%s", iRow, colname))
//				}
//				switch f.Kind() {
//				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//					n, err := strconv.ParseInt(valstr, 0, 64)
//					if err != nil {
//						return NewErr(err)
//					}
//					f.SetInt(n)
//				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
//					n, err := strconv.ParseUint(valstr, 0, 64)
//					if err != nil {
//						return NewErr(err)
//					}
//					f.SetUint(n)
//				case reflect.Float32, reflect.Float64:
//					n, err := strconv.ParseFloat(valstr, 64)
//					if err != nil {
//						return NewErr(err)
//					}
//					f.SetFloat(n)
//				case reflect.Bool:
//					n, err := strconv.ParseBool(valstr)
//					if err != nil {
//						return NewErr(err)
//					}
//					f.SetBool(n)
//				case reflect.String:
//					f.SetString(valstr)
//				}
//			} else {
//				glog.Errorf("col not found: file=%s, col=%s, struct=%s", file, colname, rowObjType.Name())
//			}
//		}

//		keys := make([]string, len(keycolidxs))
//		for i, v := range keycolidxs {
//			keys[i] = row[v]
//		}
//		v.SetMapIndex(reflect.ValueOf(strings.Join(keys, ",")), rowobjValue)

//		row, err = reader.Read()
//	}

//	return nil
//}

func LoadCsvArray(file string, slicePtr interface{}) (err error) {
	//type check
	value := reflect.ValueOf(slicePtr).Elem()
	if value.Kind() != reflect.Slice {
		return NewErrStr("arg slice must be slice type")
	}

	elemType := value.Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return NewErrStr("arg slice must be slice of struct")
	}

	//parse csv
	f, err := os.Open(file)
	if err != nil {
		return NewErr(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	//read first row
	firstrow, err := reader.Read()
	if err != nil {
		return NewErr(err)
	}

	//
	elems := reflect.MakeSlice(value.Type(), 0, 16)

	row, err := reader.Read()
	for irow := 0; row != nil; irow++ {
		if err != nil {
			return NewErr(err)
		}
		rowobjValue := reflect.New(elemType).Elem()
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
		elems = reflect.Append(elems, rowobjValue)
		row, err = reader.Read()
	}

	value.Set(elems)

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
	if err == nil {
		return nil
	}
	_, file, line, _ := runtime.Caller(1)
	return errors.New(fmt.Sprintf("%s\n\t%s : %d", err.Error(), file, line))
}

func NewErrStr(err string) error {
	_, file, line, _ := runtime.Caller(1)
	return errors.New(fmt.Sprintf("%s\n\t%s : %d", err, file, line))
}

func GenSerial(rc redis.Conn, key string, num uint32) (uint64, error) {
	out, err := redis.Int64(rc.Do("incrby", fmt.Sprintf("serial/%s", key), num))
	return uint64(out) - uint64(num) + 1, err
}
