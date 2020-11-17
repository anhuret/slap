package slap

import (
	"reflect"
	"strconv"

	"github.com/rs/xid"
)

// Create ...
func (p *Pivot) Create(data interface{}) ([]string, error) {
	if reflect.TypeOf(data).Kind() != reflect.Ptr {
		return nil, ErrInvalidParameter
	}

	var acc []interface{}
	var ids []string

	if reflect.TypeOf(data).Elem().Kind() != reflect.Slice {
		acc = append(acc, data)
	} else {
		v := reflect.ValueOf(data).Elem()
		for i := 0; i < v.Len(); i++ {
			acc = append(acc, v.Index(i).Interface())
		}
	}

	for _, d := range acc {

		s := model(d)
		if s == nil {
			return nil, ErrInvalidParameter
		}

		k := key{
			schema: p.schema,
			bucket: s.bucket,
			id:     xid.New().String(),
		}

		for f, v := range s.fields {
			k.field = f
			err := put(p.db, genKey(&k), v)
			if err != nil {
				return nil, err
			}
		}

		ids = append(ids, k.id)
	}

	return ids, nil
}

func (p *Pivot) Read(data interface{}, ids ...string) ([]interface{}, error) {
	typ := reflect.TypeOf(data)
	if typ.Kind() != reflect.Ptr && typ.Elem().Kind() != reflect.Struct {
		return nil, ErrInvalidParameter
	}

	s := model(data)
	if s == nil {
		return nil, ErrInvalidParameter
	}

	var rec []interface{}

	for _, id := range ids {
		str := reflect.New(reflect.TypeOf(data).Elem()).Elem()

		for f := range s.fields {
			k := key{
				schema: p.schema,
				bucket: s.bucket,
				id:     id,
				field:  f,
			}

			res, err := get(p.db, genKey(&k))
			if err != nil {
				return nil, err
			}

			fld := str.FieldByName(f)

			switch fld.Type().Kind() {
			case reflect.String:
				fld.SetString(res)
			case reflect.Int64:
				num, err := strconv.Atoi(res)
				if err != nil {
					return nil, err
				}
				fld.SetInt(int64(num))
			default:
				return nil, ErrInvalidParameter
			}
		}

		rec = append(rec, str.Interface())
	}

	return rec, nil
}
