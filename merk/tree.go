package merk

import (
	"bytes"
	"fmt"
	"github.com/lithdew/bytesutil"
	"unsafe"
)

type Tree struct {
	kv    *KV
	left  Link
	right Link
}

func newTree(key, value []byte) *Tree {
	return &Tree{
		kv: newKV(key, value),
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
	}
	return t.right
}

func (t *Tree) setLink(isLeft bool, link Link) {
	if isLeft {
		t.left = link
		return
	}
	t.right = link
}

func (t *Tree) child(isLeft bool) *Tree {
	var l Link = t.link(isLeft)
	if l == nil {
		return nil
	}

	if l.linkType() == PrunedLink {
		var h Hash = l.hash()
		child, err := gDB.fetchTree(h[:])
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to fetch node: %v", err))
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
	if maybeChild == nil {
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
		var h Hash = slot.hash()
		child, err := gDB.fetchTree(h[:])
		if err != nil {
			panic(fmt.Sprintf("failed to fetch node: %v", err))
		}
		return child
	}

	return slot.tree()
}

func (t *Tree) detachExpect(isLeft bool) (maybeChild *Tree) {
	maybeChild = t.detach(isLeft)

	if maybeChild == nil {
		panic(fmt.Sprintf("Expected tree to have %v child, but got Nil", sideToStr(isLeft)))
	}

	return
}

func (t *Tree) walk(isLeft bool, f func(tree *Tree) (*Tree, error)) error {
	appliedTree, err := f(t.detach(isLeft))
	if err != nil {
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
	t.kv.value = value
	t.kv.hash = kvHash(t.kv.key, value)
}

func (t *Tree) commit(c *Commiter) error {
	commitHandler(t, c, ModifiedLink)

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

func (t *Tree) commitsSnapshot(c *Commiter) error {
	return commitHandler(t, c, StoredLink)
}

func (t *Tree) verify() error {
	handler := func(l Link, compare func(l Link) bool) error {
		if l != nil {
			if compare(l) {
				return fmt.Errorf("unbalanced tree :%v", t.key())
			}
			if l.linkType() != PrunedLink {
				if err := l.tree().verify(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	err := handler(t.link(true), func(l Link) bool { return string(t.key()) <= string(l.key()) })
	if err != nil {
		return err
	}

	return handler(t.link(false), func(l Link) bool { return string(t.key()) >= string(l.key()) })
}

func (t *Tree) marshal(dst []byte) []byte {
	var hash Hash

	// write key
	dst = bytesutil.AppendUint32BE(dst, uint32(len(t.key())))
	dst = append(dst, t.key()...)

	// write left link
	if t.link(true) != nil {
		dst = append(dst, uint8(1))
		hash = t.link(true).hash()
		dst = append(dst, hash[:]...)
	} else {
		dst = append(dst, uint8(0))
	}

	// write right link
	if t.link(false) != nil {
		dst = append(dst, uint8(1))
		hash = t.link(false).hash()
		dst = append(dst, hash[:]...)
	} else {
		dst = append(dst, uint8(0))
	}

	// write value
	dst = append(dst, t.value()...)

	return dst
}

func unmarshalTree(buf []byte) *Tree {
	var (
		kLen              uint32
		hasLeft, hasRight uint8
		hash              Hash
	)

	t := &Tree{kv: &KV{}}

	// read key
	kLen, buf = bytesutil.Uint32BE(buf[:4]), buf[4:]
	t.kv.key, buf = buf[:kLen], buf[kLen:]

	// read left
	hasLeft, buf = uint8(buf[0]), buf[1:]
	if hasLeft == 1 {
		hash, buf = *(*Hash)(unsafe.Pointer(&((buf[:HashSize])[0]))), buf[HashSize:]
		t.left = &Pruned{h: hash}
	}

	// read right
	hasRight, buf = uint8(buf[0]), buf[1:]
	if hasRight == 1 {
		hash, buf = *(*Hash)(unsafe.Pointer(&((buf[:HashSize])[0]))), buf[HashSize:]
		t.right = &Pruned{h: hash}
	}

	// read value
	t.kv.value = buf

	// calculate hash
	t.kv.hash = kvHash(t.kv.key, t.kv.value)

	return t
}

func commitHandler(t *Tree, c *Commiter, lType LinkType) error {
	// Note: if use concurency, slow down when low spec pc
	chErr := make(chan error, 2)

	handler := func(l Link, isLeft bool) {
		if l != nil && l.linkType() == lType {
			go func() {

				if err := l.tree().commit(c); err != nil {
					chErr <- err
					return
				}

				t.setLink(isLeft, &Stored{
					ch: l.childHeights(),
					t:  l.tree(),
					h:  l.tree().hash(),
				})

				chErr <- nil
			}()
		} else {
			chErr <- nil
		}
	}

	handler(t.link(true), true)
	handler(t.link(false), false)

	for i := 0; i < cap(chErr); i++ {
		if err := <-chErr; err != nil {
			return err
		}
	}

	if err := c.write(t); err != nil {
		return err
	}

	return nil
}

func sideToStr(isLeft bool) string {
	if isLeft {
		return "left"
	} else {
		return "right"
	}
}
