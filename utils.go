package slap

import (
	"fmt"
	"reflect"
	"strings"
)

// Shape ...
type shape struct {
	bucket string
	fields map[string]string
}

// Model ...
func model(data interface{}) *shape {
	var res reflect.Value
	var name string
	fields := make(map[string]string)

	typ := reflect.TypeOf(data)
	val := reflect.ValueOf(data)

	switch {
	case typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct:
		res = val.Elem()
		name = typ.Elem().Name()
	case typ.Kind() == reflect.Struct:
		res = val
		name = typ.Name()
	default:
		return nil
	}

	for i := 0; i < res.NumField(); i++ {
		n := res.Type().Field(i).Name
		v := fmt.Sprintf("%v", res.Field(i).Interface())
		fields[n] = v
	}

	return &shape{
		bucket: name,
		fields: fields,
	}
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
