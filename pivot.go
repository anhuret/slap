package slap

import (
	"errors"
	"log"

	"github.com/dgraph-io/badger/v2"
)

// Pivot ...
type Pivot struct {
	db     *badger.DB
	schema string
}

type null struct{}

var (
	// ErrInvalidParameter ...
	ErrInvalidParameter = errors.New("invalid parameter")
	// ErrTypeConversion ...
	ErrTypeConversion = errors.New("type conversion")
	// ErrNoRecord ...
	ErrNoRecord = errors.New("record does not exist")
	void        null
)

const (
	_indexSchema string = "system.index"
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
