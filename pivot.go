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

var (
	// ErrInvalidParameter ...
	ErrInvalidParameter = errors.New("invalid parameter")
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
