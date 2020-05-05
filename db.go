package merk

import (
	badger "github.com/dgraph-io/badger/v2"
	"fmt"
)

const DefaultDBPath = "./merkdb"

func defaultDBOpts(path string) badger.Options {
	if path == "" {
		path = DefaultDBPath
	}
	// See available options
	// https://godoc.org/github.com/dgraph-io/badger#Options
	return badger.DefaultOptions(DefaultDBPath)
}

func openDB(path string) (*badger.DB, string, error) {
	ops := defaultDBOpts(path)

	db, err := badger.Open(ops)
  if err != nil {
	  return nil, ops.Dir, fmt.Errorf("Failed to open db: %w", err)
  }

  return db, ops.Dir, nil
}
