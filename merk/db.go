package merk

import (
	"errors"
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
	// "bytes"
)

// TOOD: Change to name
const DefaultDBPath = "./merkdb"

type DB struct {
	badger *badger.DB
	path   string
}

var (
	RootKey = []byte(".root")
	gDB     *DB
)

func defaultDBOpts(path string) badger.Options {
	if path == "" {
		path = DefaultDBPath
	}

	// See available options
	// https://godoc.org/github.com/dgraph-io/badger#Options
	return badger.DefaultOptions(path)
}

func openDB(path string) error {
	if gDB != nil {
		return errors.New("db already open")
	}

	ops := defaultDBOpts(path)

	db, err := badger.Open(ops)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}

	gDB = &DB{db, ops.Dir}

	return nil
}

func (db *DB) closeDB() {
	gDB = nil
	db.badger.Close()
}

func (db *DB) destroy() error {
	err := db.badger.DropAll()
	return err
}

func (db *DB) getItem(key []byte) ([]byte, error) {
	var copy []byte

	if err := db.badger.View(func(txn *badger.Txn) error {

		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("failed get key: %w", err)
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed item value: %w", err)
		}

		copy = append([]byte{}, val...)

		return nil

	}); err != nil {
		return nil, fmt.Errorf("failed db View: %w", err)
	}

	return copy, nil
}

func (db *DB) newBatch() *badger.WriteBatch {
	return db.badger.NewWriteBatch()
}

func (db *DB) fetchTree(key []byte) (*Tree, error) {
	if key == nil {
		return nil, errors.New("empty key while fetching tree")
	}

	item, err := db.getItem(key)
	if err != nil {
		return nil, fmt.Errorf("failed getItem: %w", err)
	}

	t, err := unmarshalTree(key, item)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalTree: %w", err)
	}

	return t, nil
}

func (db *DB) fetchTrees(key []byte) (*Tree, error) {
	tree, err := db.fetchTree(key)
	if err != nil {
		return nil, err
	}

	var leftLink *Link = tree.link(true)
	if leftLink != nil {
		leftTree, err := db.fetchTrees(leftLink.key)
		if err != nil {
			return nil, err
		}
		leftLink = leftLink.intoStored(leftTree)
	}

	var rightLink *Link = tree.link(false)
	if rightLink != nil {
		rightTree, err := db.fetchTrees(rightLink.key)
		if err != nil {
			return nil, err
		}
		rightLink = rightLink.intoStored(rightTree)
	}

	return tree, nil
}
