package slap

import (
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
	dks := k.out()

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
