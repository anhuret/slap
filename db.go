package slap

import (
	"strings"

	"github.com/dgraph-io/badger/v2"
)

func initDB(path string) (*badger.DB, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return db, nil
}

func put(db *badger.DB, k *key, value []byte) error {
	dks := strings.Join([]string{k.schema, k.bucket, k.id, k.field}, ":")

	err := db.Update(func(txn *badger.Txn) error {
		if k.index {
			item, err := txn.Get([]byte(dks))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err == nil {
				ov, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}

				oi := strings.Join([]string{_indexSchema, k.bucket, k.field, string(ov), k.id}, ":")
				err = txn.Delete([]byte(oi))
				if err != nil {
					return err
				}
			}
			ni := strings.Join([]string{_indexSchema, k.bucket, k.field, string(value), k.id}, ":")
			err = txn.Set([]byte(ni), []byte{0})
			if err != nil {
				return err
			}
		}
		err := txn.Set([]byte(dks), value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func get(db *badger.DB, key string) ([]byte, error) {
	var value []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}
