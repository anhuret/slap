package slap

import (
	"reflect"
	"testing"
)

func TestModel(t *testing.T) {
	type table struct {
		Address string
		Name    string
		Age     int64
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
			t.Error(err)
		}

		res, err := piv.Read(&table{}, id...)
		if err != nil {
			t.Error(err)
		}

		a := reflect.DeepEqual(res[0], tbl1)
		if !a {
			t.Error("tables must be equal")
		}

	})

}
