package slap

import (
	"reflect"
	"strings"

	"github.com/anhuret/gset"
	"github.com/dgraph-io/badger/v2"
	"github.com/rs/xid"
)

// Write ...
func (p *Pivot) Write(data interface{}) ([]string, error) {
	ids := []string{}
	val := reflect.ValueOf(data)
	if val.Type().Kind() != reflect.Ptr {
		return ids, ErrInvalidParameter
	}
	ind := reflect.Indirect(val)
	kin := ind.Type().Kind()

	switch kin {
	case reflect.Struct:
		s := model(ind.Interface(), false)
		if s == nil {
			return ids, ErrInvalidParameter
		}

		v := s.values(ind.Interface())
		if v == nil {
			return ids, ErrTypeConversion
		}

		id, err := p.write(s, v)
		if err != nil {
			return ids, err
		}

		return append(ids, id), nil
	case reflect.Slice:
		if ind.Len() == 0 {
			return ids, nil
		}
		s := model(ind.Index(0).Interface(), false)
		if s == nil {
			return ids, ErrInvalidParameter
		}

		var v vals
		for i := 0; i < ind.Len(); i++ {
			v = s.values(ind.Index(i).Interface())
			if v == nil {
				return ids, ErrTypeConversion
			}
			id, err := p.write(s, v)
			if err != nil {
				return ids, err
			}
			ids = append(ids, id)
		}
	default:
		return ids, ErrInvalidParameter

	}
	return ids, nil
}

// write ...
func (p *Pivot) write(s *shape, v vals) (string, error) {

	k := key{
		schema: p.schema,
		table:  s.cast.Name(),
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
		table:  s.cast.Name(),
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
		table:  s.cast.Name(),
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
	rec := []interface{}{}
	s := model(data, true)
	if s == nil {
		return rec, ErrInvalidParameter
	}

	for _, id := range ids {
		x, err := p.read(s, id)
		if err != nil {
			return rec, err
		}
		if x == nil {
			return rec, nil
		}
		rec = append(rec, x)
	}

	return rec, nil
}

// read ...
func (p *Pivot) read(s *shape, id string) (interface{}, error) {
	var nul bool
	obj := reflect.New(s.cast).Elem()

	for f, t := range s.fields {
		k := key{
			schema: p.schema,
			table:  s.cast.Name(),
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
		table:  s.cast.Name(),
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
