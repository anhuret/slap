package slap

import (
	"reflect"
	"strings"

	"github.com/anhuret/gset"
	"github.com/dgraph-io/badger/v2"
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
		//	err := put(p.db, &k, bts)
		//	if err != nil {
		//	return nil, err
		//}

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

// Delete ...
func (p *Pivot) Delete(data interface{}, ids ...string) error {
	s := model(data, true)
	if s == nil {
		return ErrInvalidParameter
	}

	k := key{
		schema: p.schema,
		bucket: s.bucket,
	}

	for _, id := range ids {
		k.id = id

		for f := range s.fields {
			k.field = f

			err := rem(p.db, &k)
			if err != nil {
				return err
			}
		}
	}

	return nil
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

	rec := make([]interface{}, 0)
	var nul bool

	for _, id := range ids {
		obj := reflect.New(reflect.TypeOf(data).Elem()).Elem()
		//obj := reflect.Zero(reflect.TypeOf(data).Elem())

		for f, t := range s.fields {
			k := key{
				schema: p.schema,
				bucket: s.bucket,
				id:     id,
				field:  f,
			}

			res, err := get(p.db, k.fld())
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			nul = true
			fld := obj.FieldByName(f)

			x, err := fromBytes(res, t)
			if err != nil {
				return nil, err
			}
			fld.Set(reflect.ValueOf(x))
		}

		if nul {
			rec = append(rec, obj.Interface())
		}
		nul = false

	}

	return rec, nil
}

func (p *Pivot) where(x interface{}) ([]string, error) {
	s := model(x, false)
	if s == nil {
		return nil, ErrInvalidParameter
	}
	v := s.values(x)
	if v == nil {
		return nil, ErrTypeConversion
	}

	k := key{
		schema: p.schema,
		bucket: s.bucket,
	}

	var acc []*gset.Set

	for f := range s.fields {
		k.field = f

		bts, err := toBytes(v[f])
		if err != nil {
			return nil, err
		}

		//stub := strings.Join([]string{_indexSchema, k.bucket, k.field, string(bts), ""}, ":")
		res := scan(p.db, k.stb(bts))
		set := gset.New()
		for _, k := range res {
			i := strings.Split(k, ":")
			set.Add(i[len(i)-1])
		}

		acc = append(acc, set)
	}

	switch len(acc) {
	case 1:
		return acc[0].ToSlice(), nil
	case 0:
		return []string{}, nil
	default:
		return acc[0].Intersect(acc[1:]...).ToSlice(), nil
	}
}

// Select ...
func (p *Pivot) Select(x interface{}) ([]interface{}, error) {
	val := reflect.Indirect(reflect.ValueOf(x)).Interface()
	ids, err := p.where(val)
	if err != nil {
		return nil, err
	}

	obs, err := p.Read(x, ids...)
	if err != nil {
		return nil, err
	}

	return obs, nil
}
