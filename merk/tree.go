package merk

import (
	// "sync"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"math"
)

type Tree struct {
	kv    *KV
	left  Link
	right Link
	// mu sync.Mutex
}

func newTree(key, value []byte) *Tree {
	return &Tree{
		kv: newKV(key, value),
	}
}

func treeFromFields(key, value []byte, hash Hash, left, right Link) *Tree {
	kv := kvFromFields(key, value, hash)
	return &Tree{
		kv:    kv,
		left:  left,
		right: right,
	}
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

func (t *Tree) link(isLeft bool) Link {
	if isLeft {
		return t.left
	} else {
		return t.right
	}
}

func (t *Tree) setLink(isLeft bool, link Link) {
	// t.mu.Lock()
	// defer t.mu.Unlock()

	if isLeft {
		t.left = link
	} else {
		t.right = link
	}
}

func (t *Tree) child(isLeft bool) *Tree {
	var l Link = t.link(isLeft)
	if l == nil {
		return nil
	}

	if l.linkType() == PrunedLink {
		child, err := gDB.fetchTree(l.key())
		if err != nil {
			fmt.Errorf("failed to fetch node: %w", err)
		}
		return child
	}

	return l.tree()
}

func (t *Tree) childHash(isLeft bool) Hash {
	var l Link = t.link(isLeft)
	if l == nil {
		return NullHash
	}

	return l.hash()
}

func (t *Tree) hash() Hash {
	return nodeHash(t.kvHash(), t.childHash(true), t.childHash(false))
}

func (t *Tree) childHeight(isLeft bool) uint8 {
	var l Link = t.link(isLeft)
	if l == nil {
		return 0
	}
	return l.height()
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

func (t *Tree) attach(isLeft bool, maybeChild *Tree) {
	if t == nil || maybeChild == nil {
		return
	}

	if bytes.Equal(maybeChild.key(), t.key()) {
		panic(fmt.Sprintf("BUG: tried to attach tree with same key, %v", t.key()))
	}

	if t.link(isLeft) != nil {
		panic(fmt.Sprintf("BUG: tried to attach to %v tree slot, but it is already Some", sideToStr(isLeft)))
	}

	slot := fromModifiedTree(maybeChild)
	t.setLink(isLeft, slot)

	return
}

func (t *Tree) detach(isLeft bool) *Tree {
	var slot Link = t.link(isLeft)
	if slot == nil {
		return nil
	}

	t.setLink(isLeft, nil)

	if slot.linkType() == PrunedLink {
		child, err := gDB.fetchTree(slot.key())
		if err != nil {
			fmt.Errorf("failed to fetch node: %w", err)
		}
		return child
	}

	return slot.tree()
}

func (t *Tree) detachExpect(isLeft bool) (maybeChild *Tree) {
	maybeChild = t.detach(isLeft)

	if maybeChild == nil {
		fmt.Errorf("Expected tree to have %v child, but got Nil", sideToStr(isLeft))
	}

	return
}

func (t *Tree) walk(isLeft bool, f func(tree *Tree) (*Tree, error)) error {
	var maybeChild *Tree = t.detach(isLeft)

	appliedTree, err := f(maybeChild)
	if err !=nil {
		return err
	}

	t.attach(isLeft, appliedTree)

	return nil
}

func (t *Tree) walkExpect(isLeft bool, f func(tree *Tree) *Tree) {
	var child *Tree = t.detachExpect(isLeft)
	t.attach(isLeft, f(child))
}

func (t *Tree) withValue(value []byte) {
	// t.mu.Lock()
	// defer t.mu.Unlock()

	t.kv.value = value
}

func (t *Tree) commit(c *Commiter) error {
	var left Link = t.link(true)
	if left != nil && left.linkType() == ModifiedLink {
		if err := left.tree().commit(c); err != nil {
			return err
		}
		t.setLink(true, &Stored{
			ch: left.childHeights(),
			t:  left.tree(),
			h:  left.tree().hash(),
		})
	}

	var right Link = t.link(false)
	if right != nil && right.linkType() == ModifiedLink {
		if err := right.tree().commit(c); err != nil {
			return err
		}
		t.setLink(false, &Stored{
			ch: right.childHeights(),
			t:  right.tree(),
			h:  right.tree().hash(),
		})
	}

	if err := c.write(t); err != nil {
		return err
	}

	if doPrune := c.prune(t); doPrune {
		if t.link(true) != nil {
			t.left = t.left.intoPruned()
		}
		if t.link(false) != nil {
			t.right = t.right.intoPruned()
		}
	}

	return nil
}

func (t *Tree) verify() error {
	var left Link = t.link(true)
	if left != nil {
		if string(t.key()) <= string(left.key()) {
			return fmt.Errorf("unbalanced tree :%v", t.key())
		}
		if left.linkType() != PrunedLink {
			if err := left.tree().verify(); err != nil {
				return err
			}
		}
	}

	var right Link = t.link(false)
	if right != nil {
		if string(t.key()) >= string(right.key()) {
			return fmt.Errorf("unbalanced tree :%v", t.key())
		}
		if right.linkType() != PrunedLink {
			if err := right.tree().verify(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *Tree) marshal() ([]byte, error) {
	var (
		buf64               [8]byte
		haveLeft, haveRight uint8
	)

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// Write kv value
	if uint32(len(t.value())) > uint32(math.MaxUint32) {
		return nil, fmt.Errorf("Too long, t.value(): %v ", t.value())
	}
	binary.LittleEndian.PutUint32(buf64[:4], uint32(len(t.value())))
	if _, err := buf.Write(buf64[:4]); err != nil {
		return nil, err
	}
	if _, err := buf.Write(t.value()); err != nil {
		return nil, err
	}

	// Write kv hash
	hash := t.kvHash()
	if _, err := buf.Write(hash[:]); err != nil {
		return nil, err
	}

	// Write left link
	if t.link(true) != nil {
		haveLeft = 1
	}
	if err := buf.WriteByte(byte(haveLeft)); err != nil {
		return nil, err
	}
	if haveLeft == 1 {
		if err := t.link(true).marshal(buf); err != nil {
			return nil, err
		}
	}

	// Write right link
	if t.link(false) != nil {
		haveRight = 1
	}
	if err := buf.WriteByte(byte(haveRight)); err != nil {
		return nil, err
	}
	if haveRight == 1 {
		if err := t.link(false).marshal(buf); err != nil {
			return nil, err
		}
	}

	return append(make([]byte, 0), buf.Bytes()...), nil
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
		t.left, _ = unmarshalPruned(r)
	}

	// Read left link
	haveRight, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if uint8(haveRight) == 1 {
		t.right, _ = unmarshalPruned(r)
	}

	return t, nil
}

func sideToStr(isLeft bool) string {
	if isLeft {
		return "left"
	} else {
		return "right"
	}
}
