package avl

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	"github.com/minio/highwayhash"

	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
)

var (
	hashKey = make([]byte, 32)
)

type nodeType byte

const (
	MerkleHashSize = 16

	NodeNonLeaf nodeType = iota
	NodeLeafValue
)

type node struct {
	wroteBack bool
	depth     byte

	key, value      []byte
	id, left, right [MerkleHashSize]byte

	viewID uint64
	size   uint64

	kind              nodeType
	leftObj, rightObj *node
}

func newLeafNode(t *Tree, key, value []byte) *node {
	n := &node{
		key:   key,
		value: value,

		kind: NodeLeafValue,

		depth: 0,
		size:  1,

		viewID: t.viewID,
	}

	n.rehash()

	return n
}

func (n *node) balanceFactor(t *Tree, left *node, right *node) int {
	if left == nil {
		left = t.mustLoadLeft(n)
	}

	if right == nil {
		right = t.mustLoadRight(n)
	}

	return int(left.depth) - int(right.depth)
}

func (n *node) sync(t *Tree, left *node, right *node) {
	if left == nil {
		left = t.mustLoadLeft(n)
	}

	if right == nil {
		right = t.mustLoadRight(n)
	}

	if left.depth > right.depth {
		n.depth = left.depth + 1
	} else {
		n.depth = right.depth + 1
	}

	n.size = left.size + right.size

	if bytes.Compare(left.key, right.key) > 0 {
		n.key = left.key
	} else {
		n.key = right.key
	}
}

func (n *node) leftRotate(t *Tree) *node {
	right := t.mustLoadRight(n)

	n = n.update(t, func(node *node) {
		node.right = right.left
		node.rightObj = right.leftObj
		node.sync(t, nil, nil)
	})

	right = right.update(t, func(node *node) {
		node.left = n.id
		node.leftObj = n
		node.sync(t, nil, nil)
	})

	return right
}

func (n *node) rightRotate(t *Tree) *node {
	left := t.mustLoadLeft(n)

	n = n.update(t, func(node *node) {
		node.left = left.right
		node.leftObj = left.rightObj
		node.sync(t, nil, nil)
	})

	left = left.update(t, func(node *node) {
		node.right = n.id
		node.rightObj = n
		node.sync(t, nil, nil)
	})

	return left
}

func (n *node) rebalance(t *Tree) *node {
	left := t.mustLoadLeft(n)
	right := t.mustLoadRight(n)

	balance := n.balanceFactor(t, left, right)

	if balance > 1 {
		if left.balanceFactor(t, nil, nil) < 0 {
			n = n.update(t, func(node *node) {
				newLeft := left.leftRotate(t)
				node.left = newLeft.id
				node.leftObj = newLeft
			})
		}

		return n.rightRotate(t)
	} else if balance < -1 {
		if right.balanceFactor(t, nil, nil) > 0 {
			n = n.update(t, func(node *node) {
				newRight := right.rightRotate(t)
				node.right = newRight.id
				node.rightObj = newRight
			})
		}

		return n.leftRotate(t)
	}

	return n
}

func (n *node) insert(t *Tree, key, value []byte) *node {
	if n.kind == NodeNonLeaf {
		left := t.mustLoadLeft(n)
		right := t.mustLoadRight(n)

		if bytes.Compare(key, left.key) <= 0 {
			return n.update(t, func(node *node) {
				left = left.insert(t, key, value)

				node.left = left.id
				node.leftObj = left
				node.sync(t, left, right)
			}).rebalance(t)
		}

		return n.update(t, func(node *node) {
			right = right.insert(t, key, value)

			node.right = right.id
			node.rightObj = right
			node.sync(t, left, right)
		}).rebalance(t)
	} else if n.kind == NodeLeafValue {
		if bytes.Equal(key, n.key) {
			return n.update(t, func(node *node) {
				node.value = value
			})
		}

		out := n.update(t, func(node *node) {
			node.kind = NodeNonLeaf

			if bytes.Compare(key, n.key) < 0 {
				newLeft := newLeafNode(t, key, value)
				node.left = newLeft.id
				node.leftObj = newLeft
				node.right = n.id
				node.rightObj = n
			} else {
				node.left = n.id
				node.leftObj = n
				newRight := newLeafNode(t, key, value)
				node.right = newRight.id
				node.rightObj = newRight
			}

			node.sync(t, nil, nil)
		})
		return out
	}

	panic(errors.Errorf("avl: on insert, found an unsupported node kind %d", n.kind))
}

func (n *node) lookup(t *Tree, key []byte) ([]byte, bool) {
	if n.kind == NodeLeafValue {
		if bytes.Equal(n.key, key) {
			return n.value, true
		}

		return nil, false
	} else if n.kind == NodeNonLeaf {
		child := t.mustLoadLeft(n)

		if bytes.Compare(key, child.key) <= 0 {
			return child.lookup(t, key)
		}

		return t.mustLoadRight(n).lookup(t, key)
	}

	panic(errors.Errorf("avl: on lookup, found an unsupported node kind %d", n.kind))
}

func (n *node) delete(t *Tree, key []byte) (*node, bool) {
	if n.kind == NodeLeafValue {
		if bytes.Equal(n.key, key) {
			return nil, true
		}

		return n, false
	} else if n.kind == NodeNonLeaf {
		var deleted bool

		left := t.mustLoadLeft(n)
		right := t.mustLoadRight(n)

		if bytes.Compare(key, left.key) <= 0 {
			left, deleted = left.delete(t, key)

			if left == nil {
				return right, deleted
			}

			if deleted {
				return n.update(t, func(node *node) {
					node.left = left.id
					node.leftObj = left
					node.sync(t, left, right)
				}).rebalance(t), deleted
			}

			return n, deleted
		}

		right, deleted = right.delete(t, key)

		if right == nil {
			return left, deleted
		}

		if deleted {
			return n.update(t, func(node *node) {
				node.right = right.id
				node.rightObj = right
				node.sync(t, left, right)
			}).rebalance(t), deleted
		}

		return n, deleted
	}

	panic(errors.Errorf("avl: on delete, found an unsupported node kind %d", n.kind))
}

func (n *node) rehash() {
	n.id = n.rehashNoWrite()
}

func (n *node) rehashNoWrite() [MerkleHashSize]byte {
	buf := bytebufferpool.Get()
	if err := n.serialize(buf); err != nil {
		panic(err)
	}

	hash := highwayhash.Sum128(buf.Bytes(), hashKey)

	bytebufferpool.Put(buf)

	return hash
}

func (n *node) clone() *node {
	cloned := *n
	return &cloned
}

func (n *node) update(t *Tree, fn func(node *node)) *node {
	cpy := n.clone()
	fn(cpy)
	cpy.viewID = t.viewID
	cpy.rehash()

	if cpy.id != n.id {
		cpy.wroteBack = false
	}

	return cpy
}

func (n *node) getString() string {
	switch n.kind {
	case NodeNonLeaf:
		return "(non-leaf) " + hex.EncodeToString(n.key)
	case NodeLeafValue:
		return fmt.Sprintf("%s -> %s", hex.EncodeToString(n.key), hex.EncodeToString(n.value))
	default:
		return "(unknown)"
	}
}

func (n *node) dfs(t *Tree, allowMissingNodes bool, cb func(*node) (bool, error)) error {
	recurseInto, err := cb(n)
	if err != nil {
		return err
	}

	if !recurseInto {
		return nil
	}

	if n.kind == NodeLeafValue {
		return nil
	}

	left, err := t.loadLeft(n)
	if err != nil {
		if !allowMissingNodes {
			return err
		}
	} else if err := left.dfs(t, allowMissingNodes, cb); err != nil {
		return err
	}

	right, err := t.loadRight(n)
	if err != nil {
		if !allowMissingNodes {
			return err
		}
	} else if err := right.dfs(t, allowMissingNodes, cb); err != nil {
		return err
	}

	return nil
}

func (n *node) serialize(buf *bytebufferpool.ByteBuffer) error {
	if err := buf.WriteByte(byte(n.kind)); err != nil {
		return err
	}

	if n.kind != NodeLeafValue {
		if _, err := buf.Write(n.left[:]); err != nil {
			return err
		}

		if _, err := buf.Write(n.right[:]); err != nil {
			return err
		}
	}

	var buf64 [8]byte

	binary.LittleEndian.PutUint64(buf64[:], n.viewID)

	if _, err := buf.Write(buf64[:]); err != nil {
		return err
	}

	// Write key.
	if uint32(len(n.key)) > uint32(math.MaxUint32) {
		panic("avl: key is too long")
	}

	binary.LittleEndian.PutUint32(buf64[:4], uint32(len(n.key)))

	if _, err := buf.Write(buf64[:4]); err != nil {
		return err
	}

	if _, err := buf.Write(n.key); err != nil {
		return err
	}

	if n.kind == NodeLeafValue {
		// Write value.
		if uint32(len(n.key)) > uint32(math.MaxUint32) {
			panic("avl: value is too long")
		}

		binary.LittleEndian.PutUint32(buf64[:4], uint32(len(n.value)))

		if _, err := buf.Write(buf64[:4]); err != nil {
			return err
		}

		if _, err := buf.Write(n.value); err != nil {
			return err
		}
	}

	// Write depth.
	if err := buf.WriteByte(n.depth); err != nil {
		return err
	}

	// Write size.
	binary.LittleEndian.PutUint64(buf64[:], n.size)
	_, err := buf.Write(buf64[:])

	return err
}

func deserialize(r *bytes.Reader) (*node, error) {
	n := new(node)

	kindBuf, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	n.kind = nodeType(kindBuf)

	if n.kind != NodeLeafValue {
		if _, err := r.Read(n.left[:]); err != nil {
			return nil, err
		}

		if _, err := r.Read(n.right[:]); err != nil {
			return nil, err
		}
	}

	var buf64 [8]byte

	_, err = r.Read(buf64[:])
	if err != nil {
		return nil, err
	}

	n.viewID = binary.LittleEndian.Uint64(buf64[:])

	// Read key.
	_, err = r.Read(buf64[:4])
	if err != nil {
		return nil, err
	}

	n.key = make([]byte, binary.LittleEndian.Uint32(buf64[:4]))

	if _, err := r.Read(n.key); err != nil {
		return nil, err
	}

	if n.kind == NodeLeafValue {
		if _, err := r.Read(buf64[:4]); err != nil {
			return nil, err
		}

		n.value = make([]byte, binary.LittleEndian.Uint32(buf64[:4]))

		if _, err := r.Read(n.value); err != nil {
			return nil, err
		}
	}

	// Read depth.
	n.depth, err = r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read size.
	if _, err := r.Read(buf64[:]); err != nil {
		return nil, err
	}

	n.size = binary.LittleEndian.Uint64(buf64[:])

	n.rehash()

	return n, nil
}

func mustDeserialize(r *bytes.Reader) *node {
	n, err := deserialize(r)
	if err != nil {
		panic(err)
	}

	return n
}
