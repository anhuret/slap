package slap

import (
	"reflect"
	"strconv"
	"strings"
)

type shape struct {
	bucket string
	fields map[string]reflect.Kind
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

	for i := 0; i < val.NumField(); i++ {
		if z {
			fields[val.Type().Field(i).Name] = val.Field(i).Kind()
			continue
		}
		if val.Field(i).IsZero() {
			continue
		}
		fields[val.Type().Field(i).Name] = val.Field(i).Kind()
	}

	s := shape{
		bucket: val.Type().Name(),
		fields: fields,
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
}

func genKey(k *key) string {
	return strings.Join([]string{k.schema, k.bucket, k.id, k.field}, ":")
}

func toBytes(x interface{}, t reflect.Kind) []byte {
	switch t {
	case reflect.String:
		return []byte(x.(string))
	case reflect.Int:
		return []byte(strconv.Itoa(x.(int)))
	case reflect.Bool:
		b := "f"
		if x.(bool) {
			b = "t"
		}
		return []byte(b)
	case reflect.Slice:
		return x.([]byte)
	default:
		return nil
	}
}

func fromBytes(b []byte, t reflect.Kind) interface{} {
	switch t {
	case reflect.String:
		return string(b)
	case reflect.Int:
		i, err := strconv.Atoi(string(b))
		if err != nil {
			return nil
		}
		return i
	case reflect.Bool:
		s := string(b)
		if s == "t" {
			return true
		}
		return false
	case reflect.Slice:
		return b
	default:
		return nil
	}

}
