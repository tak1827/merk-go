package merk

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"math"
	"strings"
)

type Merk struct {
	tree *Tree
}

func New(dir string) (*Merk, DB, error) {
	db, err := newBadger(dir)
	if err != nil {
		return nil, db, fmt.Errorf("failed to open db: %w", err)
	}

	topKey, err := db.get(RootKey)
	if err != nil {
		if strings.Contains(err.Error(), "Key not found") {
			return &Merk{}, db, nil
		}
		return nil, db, err
	}

	tree, err := db.fetchTrees(topKey)
	if err != nil {
		return nil, db, fmt.Errorf("failed fetchTrees: %w", err)
	}

	return &Merk{tree}, db, nil
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
			return nil, fmt.Errorf("keys in batch must be unique, %v", batch[i].K)
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

// TODO: separate commiting
func (m *Merk) ApplyUnchecked(batch Batch) ([][]byte, error) {
	var (
		deletedKeys [][]byte
		err         error
	)

	if batch == nil {
		return nil, errors.New("empty batch")
	}

	m.tree, deletedKeys, err = applyTo(m.tree, batch)
	if err != nil {
		return nil, err
	}

	sortBytes(deletedKeys)

	// Note: don't execute for performance
	// ensure tree valance
	// if m.tree != nil {
	// 	if err := m.tree.verify(); err != nil {
	// 		return nil, err
	// 	}
	// }

	// commit if db exist
	if gDB != nil {
		m.Commit(deletedKeys)
	}

	return deletedKeys, nil
}

func (m *Merk) Commit(deletedKeys [][]byte) error {
	wb := gDB.newWriteBatch()
	defer wb.cancel()

	tree := m.tree
	if tree != nil {
		committer := newCommitter(wb, tree.height(), DafaultLevels)
		if err := tree.commit(committer); err != nil {
			return err
		}

		var h Hash = m.RootHash()
		if err := wb.put(RootKey, h[:]); err != nil {
			return err
		}

	} else {
		// empty tree, delete root
		if err := wb.delete(RootKey); err != nil {
			return err
		}
	}

	for _, key := range deletedKeys {
		if err := wb.delete(key); err != nil {
			return err
		}
	}

	// write to db
	if err := gDB.commitWriteBatch(wb); err != nil {
		return err
	}

	return nil
}

func (m *Merk) Revert(snapshotKey Hash) error {
	if gDB == nil {
		return errors.New("db is not open")
	}

	tree, err := gDB.fetchTrees(snapshotKey[:])
	spew.Dump(snapshotKey)
	spew.Dump(tree)
	if err != nil {
		return err
	}
	m.tree = tree

	return nil
}

// Take snapshot from current stored tree
func TakeDBSnapshot() (Hash, error) {
	if gDB == nil {
		return NullHash, errors.New("db is not open")
	}

	return gDB.takeSnapshot()
}
