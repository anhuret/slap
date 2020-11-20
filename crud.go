package slap

import (
	"reflect"
	"strings"

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

		s := model(d, true, true)
		if s == nil {
			return nil, ErrInvalidParameter
		}

		k := key{
			schema: p.schema,
			bucket: s.bucket,
			id:     xid.New().String(),
		}

		for f, t := range s.fields {
			if f == "ID" {
				continue
			}
			k.field = f
			err := put(p.db, genKey(&k), toBytes(s.data[f], t))
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(f, "ID") {
				if t != reflect.String {
					return nil, ErrInvalidParameter
				}
				err = p.index(&k, s.data[f].(string))
				if err != nil {
					return nil, err
				}
			}
		}

		ids = append(ids, k.id)
	}

	return ids, nil
}

// Read ...
func (p *Pivot) Read(data interface{}, ids ...string) ([]interface{}, error) {
	s := model(data, false, true)
	if s == nil {
		return nil, ErrInvalidParameter
	}

	var rec []interface{}

	for _, id := range ids {
		str := reflect.New(reflect.TypeOf(data).Elem()).Elem()

		for f, t := range s.fields {
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

			x := fromBytes(res, t)
			if x == nil {
				return nil, ErrTypeConversion
			}
			fld.Set(reflect.ValueOf(x))
		}
		rec = append(rec, str.Interface())
	}

	return rec, nil
}

// Update ...
func (p *Pivot) Update(data interface{}, ids ...string) error {
	s := model(data, true, false)
	if s == nil {
		return ErrInvalidParameter
	}

	k := key{
		schema: p.schema,
		bucket: s.bucket,
	}

	for _, id := range ids {
		k.id = id

		for f, t := range s.fields {
			k.field = f

			err := put(p.db, genKey(&k), toBytes(s.data[f], t))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
