package merk

import (
	"bytes"
	"fmt"
	"strings"
)

type Merk struct {
	tree *Tree
}

func newMerk() (*Merk, error) {
	return &Merk{}, nil
}

// Note: Keep merk single
func newMarkWithDB(name string) (*Merk, error) {
	if err := openDB(name); err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	topKey, err := gDB.getItem(RootKey)
	if err != nil {
		if strings.Contains(err.Error(), "Key not found") {
			return newMerk()
		}
		return nil, err
	}

	tree, err := gDB.fetchTrees(topKey)
	if err != nil {
		return nil, fmt.Errorf("failed fetchTrees: %w", err)
	}

	return &Merk{tree}, nil
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

func (m *Merk) apply(batch Batch) [][]byte {
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

	return m.applyUnchecked(batch)
}

func (m *Merk) applyUnchecked(batch Batch) (deletedKeys [][]byte) {
	m.tree, deletedKeys = applyTo(m.tree, batch)

	if gDB != nil {
		m.commit(deletedKeys)
	}

	sortBytes(deletedKeys)

	return
}

func (m *Merk) commit(deletedKeys [][]byte) error {
	batch := gDB.newBatch()
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
