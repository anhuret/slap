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
	fields map[string]reflect.Kind
	index  map[string]null
}

func model(x interface{}, z bool) *shape {
	val := reflect.Indirect(reflect.ValueOf(x))
	if val.Kind() != reflect.Struct {
		return nil
	}

	fields := make(map[string]reflect.Kind)
	index := make(map[string]null)

	for i := 0; i < val.Type().NumField(); i++ {
		if !z && val.Field(i).IsZero() {
			continue
		}

		f := val.Type().Field(i)
		fields[f.Name] = val.Field(i).Kind()

		if f.Tag.Get("slap") == "index" {
			index[f.Name] = void
		}
	}

	s := shape{
		cast:   val.Type(),
		fields: fields,
		index:  index,
	}

	return &s
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
func fromBytes(bts []byte, t reflect.Kind) (interface{}, error) {
	buf := bytes.NewReader(bts)
	dec := gob.NewDecoder(buf)

	switch t {
	case reflect.String:
		var x string
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case reflect.Int:
		var x int
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case reflect.Int64:
		var x int64
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case reflect.Float64:
		var x float64
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case reflect.Slice:
		var x []byte
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case reflect.Bool:
		var x bool
		err := dec.Decode(&x)
		if err != nil {
			return nil, err
		}
		return x, nil
	case reflect.Struct:
		var x time.Time
		err := x.GobDecode(bts)
		if err != nil {
			return nil, err
		}
		return x, nil
	default:
		return nil, ErrInvalidParameter
	}
}
