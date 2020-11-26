package slap

import (
	"reflect"

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

	s := model(acc[0], false)
	if s == nil {
		return nil, ErrInvalidParameter
	}

	for _, d := range acc {
		v := s.values(d)
		if v == nil {
			return nil, ErrTypeConversion
		}

		k := key{
			schema: p.schema,
			bucket: s.bucket,
			id:     xid.New().String(),
		}

		for f := range s.fields {
			if f == "ID" {
				continue
			}
			_, k.index = s.index[f]
			k.field = f

			bts, err := toBytes(v[f])
			if err != nil {
				return nil, err
			}
			err = put(p.db, &k, bts)
			if err != nil {
				return nil, err
			}
		}

		ids = append(ids, k.id)
	}

	return ids, nil
}

// Update ...
func (p *Pivot) Update(data interface{}, ids ...string) error {
	s := model(data, false)
	if s == nil {
		return ErrInvalidParameter
	}
	v := s.values(data)
	if v == nil {
		return ErrTypeConversion
	}

	k := key{
		schema: p.schema,
		bucket: s.bucket,
	}

	for _, id := range ids {
		k.id = id

		for f := range s.fields {
			if f == "ID" {
				continue
			}
			_, k.index = s.index[f]
			k.field = f

			bts, err := toBytes(v[f])
			if err != nil {
				return err
			}
			err = put(p.db, &k, bts)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Read ...
func (p *Pivot) Read(data interface{}, ids ...string) ([]interface{}, error) {
	s := model(data, true)
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

			//x := fromBytes(res, t)
			//if x == nil {
			//return nil, ErrTypeConversion
			//}

			x, err := fromBytes(res, t)
			if err != nil {
				return nil, err
			}
			fld.Set(reflect.ValueOf(x))
		}
		rec = append(rec, str.Interface())
	}

	return rec, nil
}
