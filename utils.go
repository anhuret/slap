package slap

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"strings"
	"time"
)

type shape struct {
	cast   reflect.Type
	fields map[string]string
	index  map[string]null
}

func model(x interface{}, z bool) (*shape, error) {
	val := reflect.Indirect(reflect.ValueOf(x))
	if val.Kind() != reflect.Struct {
		return nil, ErrInvalidParameter
	}
	typ := val.Type()

	fields := make(map[string]string)
	index := make(map[string]null)
	var id bool

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Name == "ID" {
			id = true
		}

		if !z && val.Field(i).IsZero() {
			continue
		}
		fields[f.Name] = val.Field(i).Type().String()

		if f.Tag.Get("slap") == "index" {
			index[f.Name] = void
		}
	}

	if !id {
		return nil, ErrNoPrimaryID
	}

	s := shape{
		cast:   val.Type(),
		fields: fields,
		index:  index,
	}

	return &s, nil
}

func (s *shape) values(x interface{}) vals {
	val := reflect.Indirect(reflect.ValueOf(x))
	if val.Kind() != reflect.Struct {
		return nil
	}

	vls := make(vals)

	for f := range s.fields {
		vls[f] = val.FieldByName(f).Interface()
	}

	return vls
}

type key struct {
	schema string
	table  string
	id     string
	field  string
	index  bool
}

func (k *key) fld() string {
	return strings.Join([]string{k.schema, k.table, k.id, k.field}, ":")
}

func (k *key) rec() string {
	return strings.Join([]string{k.schema, k.table, k.id}, ":")
}

func (k *key) inx(v []byte) string {
	return strings.Join([]string{_indexSchema, k.table, k.field, string(v), k.id}, ":")
}

func (k *key) stb(v []byte) string {
	return strings.Join([]string{_indexSchema, k.table, k.field, string(v), ""}, ":")
}

func toBytes(x interface{}) ([]byte, error) {
	var bts bytes.Buffer
	enc := gob.NewEncoder(&bts)

	err := enc.Encode(x)
	if err != nil {
		return nil, err
	}
	return bts.Bytes(), nil

}
func fromBytes(bts []byte, t string) (interface{}, error) {
	buf := bytes.NewReader(bts)
	dec := gob.NewDecoder(buf)

	switch t {
	case "string":
		var x string
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case "int":
		var x int
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case "int64":
		var x int64
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case "float64":
		var x float64
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case "[]uint8":
		var x []byte
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case "bool":
		var x bool
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case "time.Time":
		var x time.Time
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	default:
		return nil, ErrInvalidParameter
	}
}
