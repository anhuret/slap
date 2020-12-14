package slap

import (
	"errors"
	"log"
	"reflect"
	"strings"

	"github.com/anhuret/gset"
	"github.com/dgraph-io/badger/v2"
	"github.com/rs/xid"
)

// Pivot ...
type Pivot struct {
	db     *DB
	schema string
}

type null struct{}
type vals map[string]interface{}

var (
	// ErrInvalidParameter ...
	ErrInvalidParameter = errors.New("invalid parameter")
	// ErrTypeConversion ...
	ErrTypeConversion = errors.New("type conversion")
	// ErrNoRecord ...
	ErrNoRecord = errors.New("record does not exist")
	// ErrReservedWord ...
	ErrReservedWord = errors.New("reserved identifier used")
	// ErrNoPrimaryID ...
	ErrNoPrimaryID = errors.New("primary ID field does not exist")
	// ErrMalformedKey ...
	ErrMalformedKey = errors.New("malformed key or zero key fields")

	void null
)

const (
	_indexSchema string = "system.index"
)

// New ...
func New(path, schema string) *Pivot {
	if strings.HasPrefix(schema, "system") {
		log.Fatal(ErrReservedWord)
	}
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

// Key
func (p *Pivot) key(table string) *bow {
	return &bow{
		schema: p.schema,
		table:  table,
	}
}

func (p *Pivot) create(s *shape, v vals) (string, error) {

	k := bow{
		schema: p.schema,
		table:  s.cast.Name(),
		id:     xid.New().String(),
	}

	err := p.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(k.recordK()), []byte{0})
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
			}

			err = txn.Set([]byte(k.fieldK()), bts)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	return k.id, nil
}

// read ...
func (p *Pivot) read(s *shape, id string) (interface{}, error) {
	obj := reflect.New(s.cast).Elem()

	k := bow{
		schema: p.schema,
		table:  s.cast.Name(),
		id:     id,
	}

	err := p.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(k.recordK()))
		if err == badger.ErrKeyNotFound {
			return ErrNoRecord
		}
		if err != nil {
			return err
		}

		obj.FieldByName("ID").Set(reflect.ValueOf(id))

		for f, t := range s.fields {
			k.field = f

			i, err := txn.Get([]byte(k.fieldK()))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}

			fld := obj.FieldByName(f)

			err = i.Value(func(v []byte) error {
				x, err := fromBytes(v, t)
				if err != nil {
					return err
				}

				fld.Set(reflect.ValueOf(x))

				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return obj.Interface(), nil
}

func (p *Pivot) where(x interface{}) ([]string, error) {
	s, err := model(x, false)
	if err != nil {
		return nil, err
	}

	v, err := s.values(x)
	if err != nil {
		return nil, err
	}

	k := bow{
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

		res := p.db.scan(k.stubK(bts))
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
