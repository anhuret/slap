package slap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type shape struct {
	cast   reflect.Type
	name   string
	fields map[string]string
	index  map[string]null
}

func model(x interface{}, z bool) (*shape, error) {
	val := reflect.Indirect(reflect.ValueOf(x))
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model: %w", ErrInvalidParameter)
	}

	if !val.FieldByName("ID").IsValid() {
		return nil, fmt.Errorf("model: %w", ErrNoPrimaryID)
	}

	typ := val.Type()
	fields := make(map[string]string)
	index := make(map[string]null)

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		if !z && val.Field(i).IsZero() {
			continue
		}
		fields[f.Name] = val.Field(i).Type().String()

		if f.Tag.Get("slap") == "index" {
			index[f.Name] = void
		}
	}

	if _, ok := fields["ID"]; ok {
		delete(fields, "ID")
	}

	s := shape{
		cast:   typ,
		name:   typ.Name(),
		fields: fields,
		index:  index,
	}

	return &s, nil
}

func (s *shape) values(x interface{}) (vals, error) {
	val := reflect.Indirect(reflect.ValueOf(x))
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("values: %w", ErrInvalidParameter)
	}

	vls := make(vals)

	for f := range s.fields {
		vls[f] = val.FieldByName(f).Interface()
	}

	return vls, nil
}

type bow struct {
	schema string
	table  string
	id     string
	field  string
	index  bool
}

func (b *bow) fieldK() string {
	return strings.Join([]string{b.schema, b.table, b.id, b.field}, ":")
}

func (b *bow) recordK() string {
	return strings.Join([]string{b.schema, b.table, b.id}, ":")
}

func (b *bow) tableK() string {
	return strings.Join([]string{b.schema, b.table}, ":")
}

func (b *bow) indexK(v []byte) string {
	return strings.Join([]string{_indexSchema, b.table, b.field, string(v), b.id}, ":")
}

func (b *bow) stubK(v []byte) string {
	return strings.Join([]string{_indexSchema, b.table, b.field, string(v), ""}, ":")
}

func toBytes(x interface{}) ([]byte, error) {
	var bts bytes.Buffer
	enc := gob.NewEncoder(&bts)

	err := enc.Encode(x)
	if err != nil {
		return nil, fmt.Errorf("toBytes: %w", err)
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
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	case "int":
		var x int
		err := dec.Decode(&x)
		if err != nil {
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	case "int64":
		var x int64
		err := dec.Decode(&x)
		if err != nil {
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	case "float64":
		var x float64
		err := dec.Decode(&x)
		if err != nil {
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	case "[]uint8":
		var x []byte
		err := dec.Decode(&x)
		if err != nil {
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	case "bool":
		var x bool
		err := dec.Decode(&x)
		if err != nil {
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	case "time.Time":
		var x time.Time
		err := dec.Decode(&x)
		if err != nil {
			return nil, fmt.Errorf("fromBytes: %w", err)
		}
		return x, nil
	default:
		return nil, fmt.Errorf("fromBytes: %w", ErrTypeConversion)
	}
}

func (s *shape) filter(f []string) {
	if len(f) == 0 {
		return
	}

	fields := make(map[string]string)

	for _, i := range f {
		if _, ok := s.fields[i]; ok {
			fields[i] = s.fields[i]
		}
	}

	if len(fields) != 0 {
		s.fields = fields
	}
}
