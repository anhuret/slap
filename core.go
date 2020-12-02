package slap

import (
	"reflect"
)

// Write accepts struct or slice of struct pointers
// Returns slice of record IDs saved
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
		s, err := model(ind.Interface(), false)
		if err != nil {
			return ids, err
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
		s, err := model(ind.Index(0).Interface(), false)
		if err != nil {
			return ids, err
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

// Delete removes one or many records with given IDs
// Accepts a struct and variadic IDs
func (p *Pivot) Delete(data interface{}, ids ...string) error {
	s, err := model(data, true)
	if err != nil {
		return err
	}

	k := key{
		schema: p.schema,
		table:  s.cast.Name(),
	}

	for _, id := range ids {
		k.id = id

		for f := range s.fields {
			k.field = f

			err := p.db.rem(&k)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Update mofifies records with given IDs
// Non zero values are updated
func (p *Pivot) Update(data interface{}, ids ...string) error {
	s, err := model(data, false)
	if err != nil {
		return err
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
			err = p.db.put(&k, bts)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Read retrieves one or many records with given IDs
// Returns slice of interfaces
func (p *Pivot) Read(data interface{}, ids ...string) ([]interface{}, error) {
	rec := []interface{}{}
	s, err := model(data, true)
	if err != nil {
		return rec, err
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

// Select retrieves records ANDing non zero values
// Returns slice of interfaces
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
