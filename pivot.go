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
	void           null
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

// write ...
func (p *Pivot) write(s *shape, v vals) (string, error) {

	k := key{
		schema: p.schema,
		table:  s.cast.Name(),
		id:     xid.New().String(),
	}

	err := p.db.set(k.rec(), []byte{0})
	if err != nil {
		return "", err
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
		err = p.db.put(&k, bts)
		if err != nil {
			return "", err
		}
	}

	return k.id, nil
}

// read ...
func (p *Pivot) read(s *shape, id string) (interface{}, error) {
	var nul bool
	obj := reflect.New(s.cast).Elem()

	k := key{
		schema: p.schema,
		table:  s.cast.Name(),
		id:     id,
	}

	for f, t := range s.fields {
		if f == "ID" {
			continue
		}
		k.field = f

		res, err := p.db.get(k.fld())
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
		obj.FieldByName("ID").Set(reflect.ValueOf(id))
		return obj.Interface(), nil
	}

	return nil, nil
}

func (p *Pivot) where(x interface{}) ([]string, error) {
	s, err := model(x, false)
	if err != nil {
		return nil, err
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

		res := p.db.scan(k.stb(bts))
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
