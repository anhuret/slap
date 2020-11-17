package slap

import (
	"log"

	"github.com/dgraph-io/badger/v2"
)

// Shape ...
type shape struct {
	bucket string
	fields map[string]string
}

// Pivot ...
type Pivot struct {
	db     *badger.DB
	schema string
}

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
