package merk

import (
	"fmt"
	"bytes"
	"errors"
	"github.com/valyala/bytebufferpool"
	"math"
	"encoding/binary"
	"github.com/davecgh/go-spew/spew"
)

type Tree struct {
  kv    *KV
  left  *Link
  right *Link
}

func newTree(key, value []byte) *Tree {
	return &Tree{
		kv: newKV(key, value),
	}
}

func treeFromFields(key, value []byte, hash Hash, left, right *Link) *Tree {
	kv := kvFromFields(key, value, hash)
	return &Tree{kv, left, right}
}

func (t *Tree) key() []byte {
	return t.kv.key
}

func (t *Tree) value() []byte {
	return t.kv.value
}

func (t *Tree) kvHash() Hash {
	return t.kv.hash
}

func (t *Tree) link(isLeft bool) *Link {
	if isLeft {
		return t.left
	} else {
		return t.right
	}
}

func (t *Tree) setLink(isLeft bool, link *Link) {
	if isLeft {
		t.left = link
	} else {
		t.right = link
	}
}

func (t *Tree) child(isLeft bool) *Tree {
	var l *Link = t.link(isLeft)
	if l == nil {
		return nil
	}

	if l.linkType == Pruned {
		if child, err := fetchTree(globalDB, l.key); err != nil {
			panic(fmt.Sprintf("Failed to fetch node: %v", err))
		} else {
			return child
		}
	}

	return l.tree
}

func (t *Tree) childHash(isLeft bool) Hash {
	l := t.link(isLeft)
	if l == nil {
		return NullHash
	}

	if l.linkType == Modified {
		panic("Cannot get hash from modified link")
	}

	return l.hash
}

func (t *Tree) hash() Hash {
	return nodeHash(t.kvHash(), t.childHash(true), t.childHash(false))
}


func (t *Tree) childPendingWrites(isLeft bool) uint8 {
	var link *Link = t.link(isLeft)
	if link != nil && link.linkType == Modified {
		return link.pendingWrites
	} else {
		return 0
	}
}

func (t *Tree) childHeight(isLeft bool) uint8 {
	var l *Link = t.link(isLeft)
	if l != nil {
		return l.height()
	} else {
		return 0
	}
}

func (t *Tree) childHeights() [2]uint8 {
	childHeights := [2]uint8{t.childHeight(true), t.childHeight(false)}
	return childHeights
}

func (t *Tree) height() uint8 {
	heights := []uint8{t.childHeight(true), t.childHeight(false)}
	return 1 + max(heights)
}

func (t *Tree) balanceFactor() int8 {
	return int8(t.childHeight(false) - t.childHeight(true))
}

func (t *Tree) attach(isLeft bool, maybeChild *Tree) error {
	if t == nil || maybeChild == nil {
		return nil
	}

	if bytes.Equal(maybeChild.key(), t.key()) {
		return errors.New("Tried to attach tree with same key")
	}

	if t.link(isLeft) != nil {
		return fmt.Errorf("Tried to attach to %v tree slot, but it is already Some", sideToStr(isLeft))
	}

	slot := fromModifiedTree(maybeChild)
	t.setLink(isLeft, slot)

	return nil
}

func (t *Tree) detach(isLeft bool) *Tree {
	var slot *Link = t.link(isLeft)
	if slot == nil {
		return nil
	}

	t.setLink(isLeft, nil)

	switch slot.linkType {
	case Pruned:
		if child, err := fetchTree(globalDB, slot.key); err != nil {
			panic(fmt.Sprintf("Failed to fetch node: %v", err))
		} else {
			return child
		}
	case Modified:
		return slot.tree
	case Stored:
		return slot.tree
	default:
		return nil
	}
}

func (t *Tree) detachExpect(isLeft bool) (maybeChild *Tree) {
	maybeChild = t.detach(isLeft)

	if maybeChild == nil {
		panic(fmt.Sprintf("Expected tree to have %v child, but got Nil", sideToStr(isLeft)))
	}

	return
}

func (t *Tree) walk(isLeft bool, f func(tree *Tree) *Tree) {
	var maybeChild *Tree = t.detach(isLeft)
	t.attach(isLeft, f(maybeChild))
}

func (t *Tree) walkExpect(isLeft bool, f func(tree *Tree) *Tree) {
	var child *Tree = t.detachExpect(isLeft)
	t.attach(isLeft, f(child))
}

func (t *Tree) withValue(value []byte) {
	t.kv.value = value
}

func (t *Tree) commit(c *Commiter) error {
	left := t.link(true)
	if left != nil && left.linkType == Modified {
		left.tree.commit(c)
		t.setLink(true, &Link{
			linkType: Stored,
			hash: left.tree.hash(),
			tree: left.tree,
			childHeights: left.childHeights,
		})
	}

	right := t.link(false)
	if right != nil && right.linkType == Modified {
		right.tree.commit(c)
		t.setLink(false, &Link{
			linkType: Stored,
			hash: right.tree.hash(),
			tree: right.tree,
			childHeights: right.childHeights,
		})
	}

	if c.batch != nil {
		if err := c.write(t); err != nil {
			return err
		}
	}

	if doPrune := c.prune(t); doPrune {
		spew.Dump("&&&&&&&")
		spew.Dump(t)
		if t.link(true) != nil {
			t.left = t.left.intoPruned()
		}
		if t.link(false) != nil {
			t.right = t.right.intoPruned()
		}
	}

	return nil
}

func (t *Tree) load() {
	//
}

func sideToStr(isLeft bool) string {
	if isLeft {
		return "left"
	} else {
		return "right"
	}
}

func (t *Tree) marshal(buf *bytebufferpool.ByteBuffer) error {
	var (
		buf64 [8]byte
		haveLeft, haveRight uint8
	)

	// Write kv value
	if uint32(len(t.value())) > uint32(math.MaxUint32) {
		return fmt.Errorf("Too long, t.value(): %v ", t.value())
	}
	binary.LittleEndian.PutUint32(buf64[:4], uint32(len(t.value())))
	if _, err := buf.Write(buf64[:4]); err != nil {
		return err
	}
	if _, err := buf.Write(t.value()); err != nil {
		return err
	}

	// Write kv hash
	hash := t.kvHash()
	if _, err := buf.Write(hash[:]); err != nil {
		return err
	}

	// Write left link
	if t.link(true) != nil {
		haveLeft = 1
	}
	if err := buf.WriteByte(byte(haveLeft)); err != nil {
		return err
	}
	if haveLeft == 1 {
		if err := t.link(true).marshal(buf); err != nil {
			return err
		}
	}

	// Write right link
	if t.link(false) != nil {
		haveRight = 1
	}
	if err := buf.WriteByte(byte(haveRight)); err != nil {
		return err
	}
	if haveRight == 1 {
		if err := t.link(false).marshal(buf); err != nil {
			return err
		}
	}

	return nil
}

func unmarshalTree(key, data []byte) (*Tree, error) {
	var buf64 [8]byte

	r := bytes.NewReader(data)

	t := newTree(key, []byte(""))

	t.kv.key = key

	// Read value
	if _, err := r.Read(buf64[:4]); err != nil {
		return nil, err
	}
	t.kv.value = make([]byte, binary.LittleEndian.Uint32(buf64[:4]))
	if _, err := r.Read(t.kv.value); err != nil {
		return nil, err
	}

	// Read hash
	if _, err := r.Read(t.kv.hash[:]); err != nil {
		return nil, err
	}

	// Read left link
	haveLeft, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if uint8(haveLeft) == 1 {
		t.left, _ = unmarshalLink(r)
	}

	// Read left link
	haveRight, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if uint8(haveRight) == 1 {
		t.right, _ = unmarshalLink(r)
	}

	return t, nil
}
