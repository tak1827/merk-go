package merk

import (
	"bytes"
)

type Merk struct {
  tree *Tree
  // db
  // path
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
			rerurn nil // not found
		}

		var maybeChild *Tree = link.tree()
		if maybeChild == nil {
			break
		}

		cursor = maybeChild
	}

	// TODO:
	// fetch()
	return nil
}

func (m *Merk) apply(batch *Batch) {
	// ensure keys in batch are sorted and unique
	var prevKey []byte
	for key, _ := range batch {
		if bytes.Compare(key, prevKey) == -1 {
			panic("Keys in batch must be sorted")
		} else if bytes.Equal(key, prevKey) {
			panic("Keys in batch must be unique")
		}

		prevKey = key
	}

	m.applyUnchecked(batch)
}

func (m *Merk) applyUnchecked(batch *Batch) {
	m.tree, deletedKeys := applyTo(m.tree, batch)

	// m.commit(deletedKeys)
}
