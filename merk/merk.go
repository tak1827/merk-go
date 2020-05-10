package merk

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
)

type Merk struct {
	tree *Tree
}

func NewMark(name string) (*Merk, error) {
	if err := openDB(name); err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	topKey, err := gDB.getItem(RootKey)
	if err != nil {
		if strings.Contains(err.Error(), "Key not found") {
			return &Merk{}, nil
		}
		return nil, err
	}

	tree, err := gDB.fetchTrees(topKey)
	if err != nil {
		return nil, fmt.Errorf("failed fetchTrees: %w", err)
	}

	return &Merk{tree}, nil
}

func (m *Merk) Get(key []byte) []byte {
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

func (m *Merk) RootHash() Hash {
	if m.tree != nil {
		return m.tree.hash()
	} else {
		return NullHash
	}
}

func (m *Merk) Apply(batch Batch) ([][]byte, error) {
	var prevKey []byte
	for i := 0; i < len(batch); i++ {
		// ensure keys in batch are sorted and unique
		if bytes.Compare(batch[i].K, prevKey) == -1 {
			return nil, errors.New("keys in batch must be sorted")
		} else if bytes.Equal(batch[i].K, prevKey) {
			return nil, errors.New("keys in batch must be unique")
		}
		// ensure size of keys and values less than limit
		if uint32(len(batch[i].K)) > uint32(math.MaxUint32) {
			return nil, fmt.Errorf("Too long, key: %v ", batch[i].K)
		}
		if uint32(len(batch[i].V)) > uint32(math.MaxUint32) {
			return nil, fmt.Errorf("too long, value: %v ", batch[i].V)
		}

		prevKey = batch[i].K
	}

	return m.ApplyUnchecked(batch)
}

func (m *Merk) ApplyUnchecked(batch Batch) ([][]byte, error) {
	var deletedKeys [][]byte
	m.tree, deletedKeys = applyTo(m.tree, batch)

	sortBytes(deletedKeys)

	// ensure tree valance
	if m.tree != nil {
		if err := m.tree.verify(); err != nil {
			return nil, err
		}
	}

	// commit if db exist
	if gDB != nil {
		m.Commit(deletedKeys)
	}

	return deletedKeys, nil
}

func (m *Merk) Commit(deletedKeys [][]byte) error {
	wb := gDB.newBatch()
	defer wb.Cancel()

	tree := m.tree
	if tree != nil {
		committer := newCommitter(wb, tree.height(), DafaultLevels)
		if err := tree.commit(committer); err != nil {
			return err
		}

		if err := wb.Set(RootKey, tree.key()); err != nil {
			return err
		}

	} else {
		// empty tree, delete root
		if err := wb.Delete(RootKey); err != nil {
			return err
		}
	}

	for _, key := range deletedKeys {
		if err := wb.Delete(key); err != nil {
			return err
		}
	}

	// write to db
	if err := wb.Flush(); err != nil {
		return err
	}

	return nil
}
