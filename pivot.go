package slap

import (
	"errors"
	"log"
	"strings"

	"github.com/dgraph-io/badger/v2"
)

// Pivot ...
type Pivot struct {
	db     *badger.DB
	schema string
}

var (
	// ErrInvalidParameter ...
	ErrInvalidParameter = errors.New("invalid parameter")
	// ErrTypeConversion ...
	ErrTypeConversion = errors.New("type conversion")
	// ErrNoRecord ...
	ErrNoRecord = errors.New("record does not exist")
)

// New ...
func New(path, schema string) *Pivot {
	db, err := initDB(path)
	if err != nil {
		log.Fatal(err)
	}
	return &Pivot{
		db:     db,
		schema: schema,
	}
}

// Tidy ...
func (p *Pivot) Tidy() {
	p.db.Close()
}

func (p *Pivot) index(k *key, value string) error {
	k.schema = "system.index"
	s := strings.Join([]string{k.schema, k.bucket, k.field, value, k.id}, ":")
	err := put(p.db, s, []byte{0})
	if err != nil {
		return err
	}
	return nil
}
