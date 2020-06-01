package avl

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
)

var NodeKeyPrefix = []byte("@1:")
var RootKey = []byte(".root")

const DefaultCacheSize = 2048

type Tree struct {
	kv KV

	root *node

	cache *nodeLRU

	viewID uint64
}

func New(kv KV) *Tree {
	t := &Tree{kv: kv, cache: newNodeLRU(DefaultCacheSize)}

	// Load root node if it already exists.
	if buf, err := t.kv.Get(RootKey); err == nil && len(buf) == MerkleHashSize {
		var rootID [MerkleHashSize]byte

		copy(rootID[:], buf)

		t.root = t.mustLoadNode(rootID)
	}

	return t
}

func (t *Tree) Insert(key, value []byte) {
	if t.root == nil {
		t.root = newLeafNode(t, key, value)
	} else {
		t.root = t.root.insert(t, key, value)
	}
}

func (t *Tree) Lookup(k []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}

	return t.root.lookup(t, k)
}

func (t *Tree) Delete(k []byte) bool {
	if t.root == nil {
		return false
	}

	root, deleted := t.root.delete(t, k)
	t.root = root

	return deleted
}

func (t *Tree) Commit() error {
	if t.root == nil {
		// Tree is empty, so just delete the root.
		// If deleting the root fails because it doesn't exist, ignore the error.
		_ = t.kv.Delete(RootKey)

		return nil
	}

	batch := t.kv.NewWriteBatch()

	err := t.root.dfs(t, false, func(n *node) (bool, error) {
		if n.wroteBack {
			return false, nil
		}
		n.wroteBack = true
		buf := bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
		if err := n.serialize(buf); err != nil {
			return false, err
		}

		if err := batch.Put(append(NodeKeyPrefix, n.id[:]...), buf.Bytes()); err != nil {
			return false, err
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	err = t.kv.CommitWriteBatch(batch)
	if err != nil {
		return errors.Wrap(err, "failed to commit write batch to db")
	}

	// {
	// 	oldRootID, err := t.kv.Get(RootKey)

	// 	// If we want to include null roots here, getOldRoot() also needs to be fixed.
	// 	if err == nil && len(oldRootID) == MerkleHashSize {
	// 		nextOldRootIndex := t.getNextOldRootIndex()
	// 		t.setOldRoot(nextOldRootIndex, oldRootID)
	// 		t.setNextOldRootIndex(nextOldRootIndex + 1)
	// 	}
	// }

	return t.kv.Put(RootKey, t.root.id[:])
}

func (t *Tree) loadNode(id [MerkleHashSize]byte) (*node, error) {
	if n, ok := t.cache.Load(id); ok {
		return n, nil
	}

	buf, err := t.kv.Get(append(NodeKeyPrefix, id[:]...))

	if err != nil || len(buf) == 0 {
		return nil, errors.Errorf("avl: could not find node %x", id)
	}

	n := mustDeserialize(bytes.NewReader(buf))
	n.wroteBack = true
	t.cache.Put(id, n)

	return n, nil
}

func (t *Tree) mustLoadNode(id [MerkleHashSize]byte) *node {
	n, err := t.loadNode(id)
	if err != nil {
		panic(err)
	}

	return n
}

func (t *Tree) loadLeft(n *node) (*node, error) {
	if n.leftObj != nil {
		return n.leftObj, nil
	}

	ret, err := t.loadNode(n.left)
	if err != nil {
		return nil, err
	}

	n.leftObj = ret

	return ret, nil
}

func (t *Tree) loadRight(n *node) (*node, error) {
	if n.rightObj != nil {
		return n.rightObj, nil
	}

	ret, err := t.loadNode(n.right)
	if err != nil {
		return nil, err
	}

	n.rightObj = ret

	return ret, nil
}

func (t *Tree) mustLoadLeft(n *node) *node {
	if n.leftObj != nil {
		return n.leftObj
	}

	ret := t.mustLoadNode(n.left)
	n.leftObj = ret

	return ret
}

func (t *Tree) mustLoadRight(n *node) *node {
	if n.rightObj != nil {
		return n.rightObj
	}

	ret := t.mustLoadNode(n.right)
	n.rightObj = ret

	return ret
}
