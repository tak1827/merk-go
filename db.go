package merk

import (
	badger "github.com/dgraph-io/badger/v2"
	"fmt"
  "errors"
)

const DefaultDBPath = "./merkdb"

var (
	RootKey = []byte(".root")
	globalDB *badger.DB
)

func defaultDBOpts(path string) badger.Options {
	if path == "" {
		path = DefaultDBPath
	}
	// See available options
	// https://godoc.org/github.com/dgraph-io/badger#Options
	return badger.DefaultOptions(DefaultDBPath)
}

func openDB(path string) (*badger.DB, string, error) {
  if globalDB != nil {
    return nil, "", errors.New("DB already open")
  }

	ops := defaultDBOpts(path)

	db, err := badger.Open(ops)
  if err != nil {
	  return nil, ops.Dir, fmt.Errorf("Failed to open db: %w", err)
  }

  // Set db as global referecne to fetch tree
  globalDB = db

  return db, ops.Dir, nil
}

func closeDB(db *badger.DB) {
  globalDB = nil
  db.Close()
}

func fetchTree(db *badger.DB, key []byte) (*Tree, error) {
	var copy []byte

	if err := db.View(func(txn *badger.Txn) error {

  	item, err := txn.Get(key)
  	if err != nil {
  		return err
  	}

  	if err = item.Value(func(val []byte) error {
      copy = append([]byte{}, val...)
      return nil
    }); err != nil {
      return err
  	}

    return nil

  }); err != nil {
  	return nil, err
  }

  t, err := unmarshalTree(key, copy)
  if err != nil {
  	return nil, err
  }

  return t, nil
}
