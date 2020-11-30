package slap

import (
	"reflect"
	"testing"
)

func TestCrud(t *testing.T) {
	type some struct {
		Address  string `slap:"index"`
		Name     string
		Universe int64
		Age      int `slap:"index"`
		Life     bool
		Range    []byte
		Money    float64 `slap:"index"`
	}

	tbl1 := some{
		Address:  "St Leonards",
		Name:     "Jim",
		Universe: 424242,
		Age:      60,
		Life:     true,
		Range:    []byte("some bytes"),
		Money:    32.42,
	}

	tbl2 := some{
		Address:  "St Leonards",
		Name:     "Tom",
		Universe: 999,
		Age:      46,
		Life:     true,
		Range:    []byte("some bytes"),
		Money:    36.06,
	}

	tbl3 := some{
		Address:  "Jersey St",
		Universe: 1000,
		Age:      25,
		Life:     false,
		Range:    []byte("more bytes"),
		Money:    0.42,
	}

	tbl4 := some{
		Address:  "Romsey St",
		Universe: 1001,
		Age:      46,
		Life:     true,
		Range:    []byte("if any bytes"),
		Money:    100.01,
	}

	piv := New("/tmp/badger", "sparkle")
	defer piv.db.Close()

	sl := []some{tbl1, tbl2, tbl3, tbl4}
	ws := []string{"one", "two"}
	var err error

	t.Run("test create", func(t *testing.T) {
		piv.db.DropAll()

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
		_, err = piv.Create(ws)
		if err == nil {
			t.Error("must return error")
		}
		_, err = piv.Create(&ws)
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
	})

	t.Run("test read", func(t *testing.T) {
		piv.db.DropAll()

		id, err := piv.Create(&tbl1)
		if err != nil {
			t.Fatal(err)
		}
		if id == nil {
			t.Fatal("id should not be nil")
		}

		res, err := piv.Read(&some{}, id...)
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

		if res[0].(some).Name != "Jim" {
			t.Error("invalid read field")
		}

		err = piv.Update(&some{Name: "Jack"}, id[0])
		if err != nil {
			t.Error(err)
		}

		res, err = piv.Read(&some{}, id[0])
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
		if res[0].(some).Name != "Jack" {
			t.Error("invalid update field")
		}

	})

	t.Run("test model", func(t *testing.T) {

		m := model(&tbl1, true)
		if m == nil {
			t.Error("should not be nil")
		}
		v := m.values(&tbl1)
		if v == nil {
			t.Error("should not be nil")
		}
		if v["Address"].(string) != "St Leonards" {
			t.Error("value conversion")
		}
		if v["Money"].(float64) != 32.42 {
			t.Error("value conversion")
		}

		m = model(&tbl3, false)
		if m == nil {
			t.Error("should not be nil")
		}
		v = m.values(&tbl3)
		if v == nil {
			t.Error("should not be nil")
		}
		if _, ok := v["Name"]; ok {
			t.Error("zero value present")
		}

		m = model(&tbl4, true)
		if m == nil {
			t.Error("should not be nil")
		}
		v = m.values(&tbl4)
		if v == nil {
			t.Error("should not be nil")
		}
		if v["Name"].(string) != "" {
			t.Error("value conversion")
		}

	})

	t.Run("test index", func(t *testing.T) {
		piv.db.DropAll()

		_, err = piv.Create(&sl)
		if err != nil {
			t.Error(err)
		}

		res, err := piv.where(some{Address: "Romsey St"})

		rd, err := piv.Read(&some{}, res...)
		if err != nil {
			t.Fatal(err)
		}
		if rd == nil {
			t.Fatal("res should not be nil")
		}

		a := reflect.DeepEqual(rd[0], tbl4)
		if !a {
			t.Error("tables must be equal")
		}

		if rd[0].(some).Money != 100.01 {
			t.Error("invalid read field")
		}

	})

	t.Run("test select", func(t *testing.T) {
		piv.db.DropAll()

		_, err := piv.Create(&sl)
		if err != nil {
			t.Error(err)
		}

		res, err := piv.Select(&some{Address: "St Leonards"})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("res should not be nil")
		}
		if len(res) != 2 {
			t.Fatal("res should have 2 elements")
		}

		res, err = piv.Select(&some{Address: "St Leonards", Age: 46})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("res should not be nil")
		}
		if len(res) != 1 {
			t.Fatal("res should have 1 elements")
		}

		res, err = piv.Select(&some{Address: "Romsey St"})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil {
			t.Fatal("res should not be nil")
		}
		if len(res) != 1 {
			t.Fatal("res should have 1 elements")
		}

	})

}

func TestEncoding(t *testing.T) {
	ss := "Hello, World"
	v, err := toBytes(ss)
	if err != nil {
		t.Error(err)
	}
	r, err := fromBytes(v, reflect.String)
	if err != nil {
		t.Error(err)
	}
	if ss != r.(string) {
		t.Error("invalid conversion")
	}
	si := 42
	v, err = toBytes(si)
	if err != nil {
		t.Error(err)
	}
	r, err = fromBytes(v, reflect.Int)
	if err != nil {
		t.Error(err)
	}
	if si != r.(int) {
		t.Error("invalid conversion")
	}
	bl := true
	v, err = toBytes(bl)
	if err != nil {
		t.Error(err)
	}
	r, err = fromBytes(v, reflect.Bool)
	if err != nil {
		t.Error(err)
	}
	if bl != r.(bool) {
		t.Error("invalid conversion")
	}
	bs := []byte("some bytes")
	v, err = toBytes(bs)
	if err != nil {
		t.Error(err)
	}
	r, err = fromBytes(v, reflect.Slice)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(bs, r.([]byte)) {
		t.Error("invalid conversion")
	}
	fl := 32.54
	v, err = toBytes(fl)
	if err != nil {
		t.Error(err)
	}
	r, err = fromBytes(v, reflect.Float64)
	if err != nil {
		t.Error(err)
	}
	if fl != r.(float64) {
		t.Error("invalid conversion")
	}
}
