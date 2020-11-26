package slap

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"strings"
)

type shape struct {
	bucket string
	fields map[string]reflect.Kind
	index  map[string]null
	data   map[string]interface{}
}

func model(x interface{}, d bool, z bool) *shape {
	val := reflect.ValueOf(x)

	switch {
	case val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct:
		val = val.Elem()
	case val.Kind() == reflect.Struct:
	default:
		return nil
	}

	fields := make(map[string]reflect.Kind)
	index := make(map[string]null)

	for i := 0; i < val.NumField(); i++ {
		if !z && val.Field(i).IsZero() {
			continue
		}
		ft := val.Type().Field(i)
		fields[ft.Name] = val.Field(i).Kind()
		if ft.Tag.Get("slap") == "index" {
			index[ft.Name] = void
		}
	}

	s := shape{
		bucket: val.Type().Name(),
		fields: fields,
		index:  index,
		data:   nil,
	}

	if d {
		data := make(map[string]interface{})
		for f := range s.fields {
			data[f] = val.FieldByName(f).Interface()
		}
		s.data = data
	}

	return &s
}

type key struct {
	schema string
	bucket string
	id     string
	field  string
	index  bool
}

func genKey(k *key) string {
	return strings.Join([]string{k.schema, k.bucket, k.id, k.field}, ":")
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
	default:
		return nil, ErrInvalidParameter
	}
}
