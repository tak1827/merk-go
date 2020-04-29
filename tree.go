package merk

import (
	"fmt"
	"bytes"
	"errors"
)

type Tree struct {
  kv *KV
  left Link
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

func (t *Tree) link(isLeft bool) *Link {
	if isLeft {
		return t.left
	} else {
		return t.right
	}
}

func (t *Tree) child(isLeft bool) *Link {
	return t.link(isLeft).tree()
}

func (t *Tree) childPendingWrites(isLeft bool) uint8 {
	var link *Link = t.link(isLeft)
	if link.linkType = Modified {
		return link.pendingWrites
	} else {
		return 0
	}
}

func (t *Tree) childHeight(isLeft bool) uint8 {
	var link *Link = t.link(isLeft)
	if link != nil {
		return link.height()
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

func (t *Tree) balanceFactor() uint8 {
	return t.childHeight(false) - t.childHeight(true)
}

func (t *Tree) attach(isLeft bool, maybeChild *Tree) error {
	if bytes.Equal(maybeChild.key(), t.key()) {
		return errors.New("Tried to attach tree with same key")
	}

	var slot *Link = t.link(isLeft)
	if slot == nil {
		return fmt.Errorf("Tried to attach to %v tree slot, but it is already Some", sideToStr(isLeft))
	}

	slot = fromModifiedTree(t)

	return nil
}

func (t *Tree) detach(isLeft bool) (child *Tree) {
	var slot *Link = t.link(isLeft)
	if slot == nil {
		return nil
	}

	switch slot.linkType {
	case Pruned:
		return
	case Modified || Stored:
		child = slot.tree
		return
	default:
		return
	}
}

func (t *Tree) detachExpect(isLeft bool) (maybeChild *Tree) {
	maybeChild = t.detach(isLeft)

	if maybeChild == nil {
		panic(fmt.Printf("Expected tree to have %v child, but got Nil", sideToStr(isLeft)))
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

func (t *Tree) commit() {
	// 
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
