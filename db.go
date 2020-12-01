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

func (db *DB) put(k *key, value []byte) error {
	dks := k.fld()

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

				err = txn.Delete([]byte(k.inx(ov)))
				if err != nil {
					return err
				}
			}

			err = txn.Set([]byte(k.inx(value)), []byte{0})
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

func (db *DB) get(key string) ([]byte, error) {
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

func (db *DB) rem(k *key) (err error) {
	f := k.fld()

	err = db.Update(func(txn *badger.Txn) (err error) {
		if k.index {
			var i *badger.Item
			i, err = txn.Get([]byte(f))
			if err != nil && err != badger.ErrKeyNotFound {
				return
			}

			if err == nil {
				var v []byte
				v, err = i.ValueCopy(nil)
				if err != nil {
					return
				}

				err = txn.Delete([]byte(k.inx(v)))
				if err != nil {
					return
				}
			}
		}
		err = txn.Delete([]byte(f))

		return
	})

	return
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
