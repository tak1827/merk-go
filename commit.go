package merk

import (
	"errors"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/valyala/bytebufferpool"
	"github.com/davecgh/go-spew/spew"
)

const DafaultLevels = 2

type Commiter struct {
	batch  *badger.WriteBatch
	height uint8
	levels uint8
}

func newCommitter(batch *badger.WriteBatch, height, levels uint8) *Commiter {
	return &Commiter{batch, height, levels}
}

func (c *Commiter) write(tree *Tree) error {
	key := tree.key()
	if key == nil {
		return errors.New("commiter batch key is nil")
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	tree.marshal(buf)

	if err := c.batch.Set(key, buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func (c *Commiter) prune(tree *Tree) bool {
	// keep N top levels of tree
	spew.Dump("**********")
	spew.Dump(c.height, tree.height())
	return c.height - tree.height() >= c.levels
}

