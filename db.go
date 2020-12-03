package slap

import (
	"github.com/dgraph-io/badger/v2"
)

// DB ...
type DB struct {
	*badger.DB
}

func initDB(path string) (*DB, error) {
	ops := badger.DefaultOptions(path)
	ops.Logger = nil
	db, err := badger.Open(ops)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func (db *DB) set(k string, v []byte) error {
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(k), v)
	})
}

func (db *DB) get(k string) ([]byte, error) {
	var v []byte

	err := db.View(func(txn *badger.Txn) error {
		i, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}

		v, err = i.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (db *DB) scan(stub string) []string {
	var acc []string
	db.View(func(txn *badger.Txn) error {
		ops := badger.DefaultIteratorOptions
		ops.PrefetchValues = false
		itr := txn.NewIterator(ops)
		defer itr.Close()
		pfx := []byte(stub)
		for itr.Seek(pfx); itr.ValidForPrefix(pfx); itr.Next() {
			acc = append(acc, string(itr.Item().Key()))
		}
		return nil
	})

	return acc
}

func (db *DB) isKey(key string) (bool, error) {
	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return nil
	})
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
