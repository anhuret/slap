package slap

import (
	"reflect"
	"testing"
)

func TestCrud(t *testing.T) {
	type table struct {
		Address string
		Name    string `slap:"index"`
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
	ws := []string{"one", "two"}
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

func TestModel(t *testing.T) {
	type some struct {
		Address  string
		Name     string
		Universe int64
		Age      int
		Life     bool
		Range    []byte
		Money    float64
	}

	tbl := some{
		Address:  "St Leonards",
		Universe: 1000,
		Age:      46,
		Life:     true,
		Range:    []byte("random bytes"),
		Money:    32.42,
	}

	m := model(&tbl, true)
	if m == nil {
		t.Error("should not be nil")
	}
	v := m.values(&tbl)
	if v == nil {
		t.Error("should not be nil")
	}
	if v["Address"].(string) != "St Leonards" {
		t.Error("value conversion")
	}
	if v["Money"].(float64) != 32.42 {
		t.Error("value conversion")
	}
	if v["Name"].(string) != "" {
		t.Error("value conversion")
	}

	m = model(&tbl, false)
	if m == nil {
		t.Error("should not be nil")
	}
	v = m.values(&tbl)
	if v == nil {
		t.Error("should not be nil")
	}
	if _, ok := v["Name"]; ok {
		t.Error("zero value present")
	}
}

func TestIndex(t *testing.T) {
	type table struct {
		Address string
		Name    string `slap:"index"`
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
	tbl3 := table{
		Address: "Church",
		Name:    "Olya",
		Age:     35,
	}
	piv := New("/tmp/badger", "sparkle")
	piv.db.DropAll()
	defer piv.Tidy()

	sl := []table{tbl1, tbl2, tbl3}

	var err error
	_, err = piv.Create(&sl)
	if err != nil {
		t.Error(err)
	}

	res, err := piv.where(table{Name: "Ruslan"})

	rd, err := piv.Read(&table{}, res...)
	if err != nil {
		t.Fatal(err)
	}
	if rd == nil {
		t.Fatal("res should not be nil")
	}

	a := reflect.DeepEqual(rd[0], tbl1)
	if !a {
		t.Error("tables must be equal")
	}

	if rd[0].(table).Name != "Ruslan" {
		t.Error("invalid read field")
	}
}
