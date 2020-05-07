package merk

import (
	"errors"
	badger "github.com/dgraph-io/badger/v2"
)

const DafaultLevels = 1

type Commiter struct {
	batch  *badger.WriteBatch
	height uint8
	levels uint8
}

func newCommitter(batch *badger.WriteBatch, height, levels uint8) *Commiter {
	return &Commiter{batch, height, levels}
}

func (c *Commiter) write(tree *Tree) error {
	var key []byte = tree.key()
	if key == nil {
		return errors.New("commiter batch key is nil")
	}

	b, err := tree.marshal()
	if err != nil {
		return err
	}

	if err := c.batch.Set(key, b); err != nil {
		return err
	}

	return nil
}

func (c *Commiter) prune(tree *Tree) bool {
	return c.height-tree.height() >= c.levels
}
