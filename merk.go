package merk

import (
	"bytes"
	badger "github.com/dgraph-io/badger/v2"
)

type Merk struct {
  tree *Tree
  db   *badger.DB
  path string
}

func newMerk(path string) (*Merk, error) {
	badger, dbPath, err := openDB(path)
	if err != nil {
		return nil, err
	}

	return &Merk{db: badger, path: dbPath}, nil
}

func (m *Merk) get(key []byte) []byte {
	if m.tree == nil {
		return nil // empty tree
	}

	var cursor *Tree = m.tree
	for {
		if bytes.Equal(key, cursor.key()) {
			return cursor.value()
		}

		var isLeft bool = bytes.Compare(key, cursor.key()) == -1
		var link *Link = cursor.link(isLeft)
		if link == nil {
			return nil // not found
		}

		var maybeChild *Tree = link.tree
		if maybeChild == nil {
			break
		}

		cursor = maybeChild
	}

	// TODO:
	// fetch()
	return nil
}

func (m *Merk) rootHash() Hash {
	if m.tree != nil {
		return m.tree.hash()
	} else {
		return NullHash
	}
}

func (m *Merk) apply(batch Batch) {
	// ensure keys in batch are sorted and unique
	var prevKey []byte
	for i := 0; i < len(batch); i++ {
		if bytes.Compare(batch[i].key, prevKey) == -1 {
			panic("Keys in batch must be sorted")
		} else if bytes.Equal(batch[i].key, prevKey) {
			panic("Keys in batch must be unique")
		}

		prevKey = batch[i].key
	}

	m.applyUnchecked(batch)
}

func (m *Merk) applyUnchecked(batch Batch) {
	// var deletedKeys [][]bytes
	m.tree, _ = applyTo(m.tree, batch)

	// m.commit(deletedKeys)
}

func (m *Merk) destroy() error {
	err := m.db.DropAll()
	return err
}

// func (m *Merk) commit(deletedKeys [][]byte) error {
// 	batch := m.db.NewWriteBatch()
// 	defer batch.Cancel()

// 	for i := 0; i < N; i++ {
// 	  err := wb.Set(key(i), value(i), 0) // Will create txns as needed.
// 	  handle(err)
// 	}
// 	handle(wb.Flush()) // Wait for all txns to finish.
// 	return nil
// }
