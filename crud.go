package slap

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v2"
	"github.com/rs/xid"
)

// Shape ...
type shape struct {
	bucket string
	fields map[string]string
}

// Pivot ...
type Pivot struct {
	db     *badger.DB
	schema string
}

// New ...
func New(path, schema string) *Pivot {
	db, err := initDB(path)
	if err != nil {
		log.Fatal(err)
	}
	return &Pivot{
		db:     db,
		schema: schema,
	}
}

// Tidy ...
func (p *Pivot) Tidy() {
	p.db.Close()
}

// Model ...
func model(data interface{}) *shape {
	var res reflect.Value
	var name string
	fields := make(map[string]string)

	typ := reflect.TypeOf(data)
	val := reflect.ValueOf(data)

	switch {
	case typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct:
		res = val.Elem()
		name = typ.Elem().Name()
	case typ.Kind() == reflect.Struct:
		res = val
		name = typ.Name()
	default:
		return nil
	}

	for i := 0; i < res.NumField(); i++ {
		n := res.Type().Field(i).Name
		v := fmt.Sprintf("%v", res.Field(i).Interface())
		fields[n] = v
	}

	return &shape{
		bucket: name,
		fields: fields,
	}
}

// Create ...
func (p *Pivot) Create(data interface{}) ([]string, error) {
	if reflect.TypeOf(data).Kind() != reflect.Ptr {
		return nil, errors.New("wrong parameter type")
	}
	var acc []interface{}
	if reflect.TypeOf(data).Elem().Kind() != reflect.Slice {
		acc = append(acc, data)
	} else {
		v := reflect.ValueOf(data).Elem()
		for i := 0; i < v.Len(); i++ {
			acc = append(acc, v.Index(i).Interface())
		}
	}

	var ids []string
	for _, d := range acc {

		s := model(d)
		if s == nil {
			return nil, errors.New("wrong parameter type")
		}
		x := xid.New().String()
		for f, v := range s.fields {
			k := strings.Join([]string{p.schema, s.bucket, x, f}, ":")
			err := put(p.db, k, v)
			if err != nil {
				return nil, err
			}
		}
		ids = append(ids, x)
	}

	return ids, nil
}

func (p *Pivot) Read(data interface{}, ids ...string) ([]interface{}, error) {
	typ := reflect.TypeOf(data)
	if typ.Kind() != reflect.Ptr && typ.Elem().Kind() != reflect.Struct {
		return nil, errors.New("wrong parameter type")
	}

	s := model(data)
	var result []interface{}

	for _, id := range ids {

		str := reflect.New(reflect.TypeOf(data).Elem()).Elem()

		for f := range s.fields {
			k := strings.Join([]string{p.schema, s.bucket, id, f}, ":")
			res, err := get(p.db, k)
			if err != nil {
				return nil, err
			}

			fv := str.FieldByName(f)

			switch fv.Type().Kind() {
			case reflect.String:
				fv.SetString(res)
			case reflect.Int64:
				num, err := strconv.Atoi(res)
				if err != nil {
					return nil, err
				}
				fv.SetInt(int64(num))
			default:
				return nil, errors.New("wrong parameter type")
			}
		}

		result = append(result, str.Interface())
	}

	return result, nil
}
