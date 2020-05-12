package merk

import (
	"github.com/valyala/bytebufferpool"
)

// TODO: Make this option
const DafaultLevels = 1

type Commiter struct {
	batch  WriteBatch
	height uint8
	levels uint8
	pool   *bytebufferpool.Pool
}

func newCommitter(batch WriteBatch, height, levels uint8) *Commiter {
	pool := new(bytebufferpool.Pool)
	return &Commiter{batch, height, levels, pool}
}

func (c *Commiter) write(tree *Tree) error {
	// Node: allow for testing
	if c.batch == nil {
		return nil
	}

	var key []byte = tree.key()
	var b []byte = tree.marshal(c.pool)

	if err := c.batch.put(append(NodeKeyPrefix, key...), b); err != nil {
		return err
	}

	return nil
}

func (c *Commiter) prune(tree *Tree) bool {
	return c.height-tree.height() >= c.levels
}
