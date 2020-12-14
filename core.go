package slap

import (
	"reflect"
	"strings"

	"github.com/dgraph-io/badger/v2"
)

// Create accepts struct or slice of struct pointers
// Returns slice of record IDs saved
func (p *Pivot) Create(data interface{}) ([]string, error) {
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

		v, err := s.values(ind.Interface())
		if err != nil {
			return ids, err
		}

		id, err := p.create(s, v)
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
			v, err = s.values(ind.Index(i).Interface())
			if err != nil {
				return ids, err
			}

			id, err := p.create(s, v)
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

	k := bow{
		schema: p.schema,
		table:  s.cast.Name(),
	}

	for _, id := range ids {
		k.id = id

		err = p.db.Update(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(k.recordK()))
			if err == badger.ErrKeyNotFound {
				return nil
			}
			if err != nil {
				return err
			}

			for f := range s.fields {
				k.field = f

				if k.index {
					i, err := txn.Get([]byte(k.fieldK()))
					if err != nil {
						return err
					}

					err = i.Value(func(v []byte) error {
						return txn.Delete([]byte(k.indexK(v)))
					})
					if err != nil {
						return err
					}
				}

				err = txn.Delete([]byte(k.fieldK()))
				if err != nil {
					return err
				}
			}
			return txn.Delete([]byte(k.recordK()))
		})
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

	v, err := s.values(data)
	if err != nil {
		return err
	}

	k := bow{
		schema: p.schema,
		table:  s.cast.Name(),
	}

	for _, id := range ids {
		k.id = id

		err = p.db.Update(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(k.recordK()))
			if err == badger.ErrKeyNotFound {
				return ErrNoRecord
			}
			if err != nil {
				return err
			}

			for f := range s.fields {
				_, k.index = s.index[f]
				k.field = f

				bts, err := toBytes(v[f])
				if err != nil {
					return err
				}

				if k.index {
					err = txn.Set([]byte(k.indexK(bts)), []byte{0})
					if err != nil {
						return err
					}

					i, err := txn.Get([]byte(k.fieldK()))
					if err != nil && err != badger.ErrKeyNotFound {
						return err
					}

					if err == nil {
						err = i.Value(func(v []byte) error {
							return txn.Delete([]byte(k.indexK(v)))
						})
						if err != nil {
							return err
						}
					}
				}

				err = txn.Set([]byte(k.fieldK()), bts)
				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// Read retrieves one or many records with given IDs
// Returns slice of interfaces
func (p *Pivot) Read(data interface{}, ftr []string, ids ...string) ([]interface{}, error) {
	rec := []interface{}{}
	s, err := model(data, true)
	if err != nil {
		return rec, err
	}

	s.filter(ftr)

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
func (p *Pivot) Select(x interface{}, ftr []string) ([]interface{}, error) {
	val := reflect.Indirect(reflect.ValueOf(x)).Interface()
	ids, err := p.where(val)
	if err != nil {
		return nil, err
	}

	obs, err := p.Read(x, ftr, ids...)
	if err != nil {
		return nil, err
	}

	return obs, nil
}

// WithDB ...
func (p *Pivot) WithDB(f func(*badger.DB) error) (err error) {
	return f(p.db.DB)
}

// Take ...
func (p *Pivot) Take(table interface{}, filter []string, seek string, limit int) ([]interface{}, error) {
	result := []interface{}{}
	shape, err := model(table, true)
	if err != nil {
		return result, err
	}

	shape.filter(filter)
	key := p.key(shape.name)

	err = p.db.View(func(txn *badger.Txn) error {
		itr := txn.NewIterator(badger.DefaultIteratorOptions)
		defer itr.Close()
		pfx := key.tableK()
		sek := pfx
		if seek != "" {
			key.id = seek
			sek = key.recordK()
		}
		ids := []string{}

		count := limit
		if limit == 0 {
			count++
		}

		for itr.Seek([]byte(sek)); itr.ValidForPrefix([]byte(pfx)) && count > 0; itr.Next() {
			k := itr.Item().Key()
			s := strings.Split(string(k), ":")
			if len(s) != 3 {
				continue
			}

			ids = append(ids, s[2])
			if limit != 0 {
				count--
			}
		}

		kst := bow{
			schema: p.schema,
			table:  shape.cast.Name(),
		}

		for _, id := range ids {
			kst.id = id
			obj := reflect.New(shape.cast).Elem()
			obj.FieldByName("ID").Set(reflect.ValueOf(id))

			for f, t := range shape.fields {
				kst.field = f

				i, err := txn.Get([]byte(kst.fieldK()))
				if err == badger.ErrKeyNotFound {
					continue
				}
				if err != nil {
					return err
				}

				err = i.Value(func(v []byte) error {
					x, err := fromBytes(v, t)
					if err != nil {
						return err
					}
					obj.FieldByName(f).Set(reflect.ValueOf(x))
					return nil
				})
				if err != nil {
					return err
				}
			}

			result = append(result, obj.Interface())
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
