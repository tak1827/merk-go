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

// Note: Keep merk single
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

		isLeft := bytes.Compare(key, cursor.key()) == -1
		maybeChild := cursor.child(isLeft)
		if maybeChild == nil {
			break // not found
		}

		cursor = maybeChild
	}

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

func (m *Merk) commit(deletedKeys [][]byte) error {
	batch := m.db.NewWriteBatch()
	defer batch.Cancel()

	tree := m.tree
	if tree != nil {
		committer := newCommitter(batch, tree.height(), DafaultLevels)
		if err := tree.commit(committer); err != nil {
			return err
		}

		if err := batch.Set(RootKey, tree.key()); err != nil {
			return err
		}

	} else {
		// empty tree, delete root
		if err := batch.Delete(RootKey); err != nil {
			return err
		}
	}

	for _, key := range deletedKeys {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}

	// write to db
	if err := batch.Flush(); err != nil {
		return err
	}

	return nil
}
