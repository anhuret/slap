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
	typof := reflect.TypeOf(data)
	var ids []string

	switch {
	case typof.Kind() == reflect.Ptr && typof.Elem().Kind() == reflect.Struct:
		id, err := p.write(data)
		if err != nil {
			return nil, err
		}
		return append(ids, id), nil
	case typof.Kind() == reflect.Ptr && typof.Elem().Kind() == reflect.Slice:
		s := reflect.Indirect(reflect.ValueOf(data))

		for i := 0; i < s.Len(); i++ {
			id, err := p.write(s.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
	default:
		return nil, ErrInvalidParameter

	}
	return ids, nil
}

// write ...
func (p *Pivot) write(data interface{}) (string, error) {
	val := reflect.Indirect(reflect.ValueOf(data))
	if val.Kind() != reflect.Struct {
		return "", ErrInvalidParameter
	}

	x := val.Interface()
	s := model(x, false)
	if s == nil {
		return "", ErrInvalidParameter
	}
	v := s.values(x)
	if v == nil {
		return "", ErrTypeConversion
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
			return "", err
		}
		err = put(p.db, &k, bts)
		if err != nil {
			return "", err
		}
	}

	return k.id, nil
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
	rec := make([]interface{}, 0)

	for _, id := range ids {
		x, err := p.read(data, id)
		if err != nil {
			return nil, err
		}
		if x == nil {
			return rec, nil
		}
		rec = append(rec, x)
	}

	return rec, nil
}

// read ...
func (p *Pivot) read(data interface{}, id string) (interface{}, error) {
	val := reflect.Indirect(reflect.ValueOf(data))
	if val.Kind() != reflect.Struct {
		return "", ErrInvalidParameter
	}

	s := model(data, true)
	if s == nil {
		return nil, ErrInvalidParameter
	}

	var nul bool
	obj := reflect.New(val.Type()).Elem()

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
		return obj.Interface(), nil
	}

	return nil, nil
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
