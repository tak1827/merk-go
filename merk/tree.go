package merk

import (
	// "sync"
	"bytes"
	"fmt"
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

// func treeFromFields(key, value []byte, hash Hash, left, right Link) *Tree {
// 	kv := kvFromFields(key, value, hash)
// 	return &Tree{
// 		kv:    kv,
// 		left:  left,
// 		right: right,
// 	}
// }

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
	// t.mu.Lock()
	// defer t.mu.Unlock()

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
		child, err := gDB.fetchTree(l.key())
		if err != nil {
			panic(fmt.Sprintf("failed to fetch node: %v", err))
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
		child, err := gDB.fetchTree(slot.key())
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
	// t.mu.Lock()
	// defer t.mu.Unlock()

	t.kv.value = value
}

func (t *Tree) commit(c *Commiter) error {
	// Note: if use concurency, slow down when low spec pc
	chErr := make(chan error, 2)

	var left Link = t.link(true)
	if left != nil && left.linkType() == ModifiedLink {
		go func() {

			if err := left.tree().commit(c); err != nil {
				chErr <- err
				return
			}

			t.setLink(true, &Stored{
				ch: left.childHeights(),
				t:  left.tree(),
				h:  left.tree().hash(),
			})

			chErr <- nil
		}()
	} else {
		chErr <- nil
	}

	var right Link = t.link(false)
	if right != nil && right.linkType() == ModifiedLink {
		go func() {
			if err := right.tree().commit(c); err != nil {
				chErr <- err
				return
			}

			t.setLink(false, &Stored{
				ch: right.childHeights(),
				t:  right.tree(),
				h:  right.tree().hash(),
			})

			chErr <- nil
		}()
	} else {
		chErr <- nil
	}

	for i := 0; i < cap(chErr); i++ {
		if err := <-chErr; err != nil {
			return err
		}
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

func sideToStr(isLeft bool) string {
	if isLeft {
		return "left"
	} else {
		return "right"
	}
}
