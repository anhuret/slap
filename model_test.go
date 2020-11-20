package slap

import (
	"reflect"
	"testing"
)

func TestModel(t *testing.T) {
	type table struct {
		Address string
		Name    string
		Age     int
	}

	tbl1 := table{
		Address: "St Leonards",
		Name:    "Ruslan",
		Age:     46,
	}
	tbl2 := table{
		Address: "Romsey",
		Name:    "Sasha",
		Age:     9,
	}
	piv := New("/tmp/badger", "sparkle")
	defer piv.Tidy()

	sl := []table{tbl1, tbl2}
	var err error
	_, err = piv.Create(&tbl1)
	if err != nil {
		t.Error(err)
	}
	_, err = piv.Create(tbl1)
	if err == nil {
		t.Error("must return error")
	}
	_, err = piv.Create(sl)
	if err == nil {
		t.Error("must return error")
	}
	_, err = piv.Create(&sl)
	if err != nil {
		t.Error(err)
	}
	_, err = piv.Create("test")
	if err == nil {
		t.Error("must return error")
	}

	t.Run("test read", func(t *testing.T) {

		id, err := piv.Create(&tbl1)
		if err != nil {
			t.Fatal(err)
		}
		if id == nil {
			t.Fatal("id should not be nil")
		}

		res, err := piv.Read(&table{}, id...)
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("res should not be nil")
		}

		a := reflect.DeepEqual(res[0], tbl1)
		if !a {
			t.Error("tables must be equal")
		}

		if res[0].(table).Name != "Ruslan" {
			t.Error("invalid read field")
		}

		err = piv.Update(&table{Name: "Jim"}, id[0])
		if err != nil {
			t.Error(err)
		}

		res, err = piv.Read(&table{}, id[0])
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("result should not be nil")
		}

		a = reflect.DeepEqual(res[0], tbl1)
		if a {
			t.Error("tables must NOT be equal")
		}
		if res[0].(table).Name != "Jim" {
			t.Error("invalid update field")
		}

	})

}

func TestModel2(t *testing.T) {
	type some struct {
		Address string
		Name    string
		Age     int
		Life    string
		Range   []byte
	}

	tbl := some{
		Address: "St Leonards",
		Name:    "Ruslan",
		Age:     46,
	}

	m := model(&tbl, true, true)
	t.Log(m)
	m1 := model(&tbl, false, true)
	t.Log(m1)
	m2 := model(&tbl, false, false)
	t.Log(m2)
	s := "Karaganda"
	v := toBytes(s, reflect.String)
	r := fromBytes(v, reflect.String)
	if s != r.(string) {
		t.Error("invalid conversion")
	}
	s2 := 42
	v = toBytes(s2, reflect.Int)
	r = fromBytes(v, reflect.Int)
	if s2 != r.(int) {
		t.Error("invalid conversion")
	}
	s3 := true
	v = toBytes(s3, reflect.Bool)
	r = fromBytes(v, reflect.Bool)
	if s3 != r.(bool) {
		t.Error("invalid conversion")
	}
	s4 := []byte("string")
	v = toBytes(s4, reflect.Slice)
	r = fromBytes(v, reflect.Slice)
	if !reflect.DeepEqual(s4, r.([]byte)) {
		t.Error("invalid conversion")
	}
}
