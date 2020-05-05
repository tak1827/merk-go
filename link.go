package merk

import (
	"fmt"
	"math"
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"bytes"
)

type LinkType uint8

const (
	Pruned LinkType = 1 << iota
	Modified
	Stored
)

// Note: linkType have specific fields
// Pruned   -> hash,           child_heights, key
// Modified -> pending_writes, child_heights, tree
// Stored   -> hash,           child_heights, tree
type Link struct {
	linkType      LinkType
	hash          Hash
	key           []byte
	childHeights  [2]uint8 // [left, right]
	pendingWrites uint8
	tree          *Tree
}

func fromModifiedTree(tree *Tree) *Link {
	var pendingWrites uint8 = 1 +
		tree.childPendingWrites(true) +
		tree.childPendingWrites(false)

	return &Link{
		linkType: Modified,
		pendingWrites: pendingWrites,
		childHeights: tree.childHeights(),
		tree: tree,
	}
}

func (l *Link) isPruned() bool {
	return l.linkType == Pruned
}

func (l *Link) isModified() bool {
	return l.linkType == Modified
}

func (l *Link) isStored() bool {
	return l.linkType == Stored
}

// func (l *Link) hash() Hash {
// 	switch l.linkType {
// 	case Modified:
// 		panic("Cannot get hash from modified link")
// 	case Pruned:
// 		return l.hash
// 	case Stored:
// 		return l.hash
// 	default:
// 		panic("link type dose not match")
// 	}
// }

func (l *Link) height() uint8 {
	return 1 + max(l.childHeights[:])
}

func (l *Link) balanceFactor() int8 {
	if l == nil {
		return 0
	}
	return int8(l.childHeights[1] - l.childHeights[0])
}

func (l *Link) intoPruned() {
	switch l.linkType {
	case Pruned:
		return
	case Modified:
		panic("Cannot prune Modified tree")
	case Stored:
		l.linkType = Pruned
		l.key = l.tree.key()
		l.tree = nil
		return
	default:
		panic("link type dose not match")
	}
}

func (l *Link) marshal(buf *bytebufferpool.ByteBuffer) error {
	var (
		buf64 [8]byte
		key []byte
	)

	switch l.linkType {
	case Pruned:
		key = l.key
	case Stored:
		key = l.tree.key()
	default:
		panic("BUG: No encoding for Link::Modified")
	}

	// Write key
	if uint32(len(key)) > uint32(math.MaxUint32) {
		return fmt.Errorf("Too long, key: %v ", key)
	}
	binary.LittleEndian.PutUint32(buf64[:4], uint32(len(key)))
	if _, err := buf.Write(buf64[:4]); err != nil {
		return err
	}
	if _, err := buf.Write(key); err != nil {
		return err
	}

	// Write hash
	if _, err := buf.Write(l.hash[:]); err != nil {
		return err
	}

	// Write child heights
	if err := buf.WriteByte(byte(l.childHeights[0])); err != nil {
		return err
	}
	if err := buf.WriteByte(byte(l.childHeights[1])); err != nil {
		return err
	}

	return nil
}

func unmarshalLink(r *bytes.Reader) (*Link, error) {
	var buf64 [8]byte

	l := new(Link)

	l.linkType = Pruned

	// Read key
	if _, err := r.Read(buf64[:4]); err != nil {
		return nil, err
	}
	l.key = make([]byte, binary.LittleEndian.Uint32(buf64[:4]))
	if _, err := r.Read(l.key); err != nil {
		return nil, err
	}

	// Read hash
	if _, err := r.Read(l.hash[:]); err != nil {
		return nil, err
	}

	// Read left child height
	left, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	l.childHeights[0] = uint8(left)

	// Read right child height
	right, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	l.childHeights[1] = uint8(right)

	return l, nil
}
