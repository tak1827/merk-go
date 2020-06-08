package merk

import (
	"github.com/valyala/bytebufferpool"
)

// TODO: Make this option
const DafaultLevels = 1

type Commiter struct {
	wb     WriteBatch
	height uint8
	levels uint8
	pool   bytebufferpool.Pool
}

func newCommitter(b WriteBatch, h, l uint8) *Commiter {
	return &Commiter{wb: b, height: h, levels: l}
}

func (c *Commiter) write(tree *Tree) error {
	// Node: allow for testing
	if c.wb == nil {
		return nil
	}

	var key Hash = tree.Hash()

	buf := c.pool.Get()
	defer c.pool.Put(buf)

	buf.B = tree.marshal(buf.B)

	value := make([]byte, len(buf.B))
	copy(value, buf.B)

	if err := c.wb.put(append(NodeKeyPrefix, key[:]...), value); err != nil {
		return err
	}

	return nil
}

func (c *Commiter) prune(tree *Tree) bool {
	return c.height-tree.height() >= c.levels
}
