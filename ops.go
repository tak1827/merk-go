package merk

import (
	"fmt"
	"math"
)

type OpType uint8

const (
	Put OpType = 1 << iota
	Delete
)

type Op struct {
	op  OpType
	key byte[]
	val byte[]
}

type Batch []*Op

func applyTo(maybeTree *Tree, batch *Batch) (*Tree, [][]byte) {
	var deletedKeys [][]byte
	if batch != nil && maybeTree != nil {
		deletedKeys = apply(maybeTree, batch)
	} else {
		return build(), nil
	}

	return maybeTree, deletedKeys
}

func build(batch *Batch) *Tree {
	if batch == nil {
		return nil
	}

	var midIndex int = len(batch) / 2
	var midKey []byte = batch[midIndex].key
	var midOp []byte = batch[midIndex].op
	var midValue []byte = batch[midIndex].val
	if midOp == Delete {
		panic(fmt.Printf("Tried to delete non-existent key %v"), midKey)
	}

	midTree := newTree(midKey, midValue)
	midTree, _ = recurse(midTree, batch, midIndex, true)

	return midTree
}

func apply(tree *Tree, batch Batch) (*Tree, [][]byte) {
	found, mid := binarySearchBy(tee.key(), batch)

	if found {
		tree.withValue(batch[mid])
	}

	var exclusive bool = found

	recurse(tree, batch, mid, exclusive)
}

func recurse(tree *Tree, batch *Batch, mid int, exclusive bool) (*Tree, [][]byte) {
	var leftBatch *Batch = batch[:mid]
	var rightBatch *Batch
	if exclusive {
		rightBatch = [mid + 1:]
	} else {
		rightBatch = [mid:]
	}

	var deletedKeys [][]byte

	if leftBatch != nil {
		tree.walk(true, func(maybeLeft *Tree) *Tree {
			maybeLeft, deletedKeysLeft := applyTo(maybeLeft, leftBatch)
	 		deletedKeys	= append(deletedKeys, deletedKeysLeft...)
	 		return maybeLeft
		})
	}

	if rightBatch != nil {
		tree.walk(true, func(maybeRight *Tree) *Tree {
			maybeRight, deletedKeysRight := applyTo(maybeRight, rightBatch)
	 		deletedKeys	= append(deletedKeys, deletedKeysRight...)
	 		return maybeRight
		})
	}

	return maybeBalance(tree), deletedKeys
}

func balanceFactor(tree *Tree) uint8 {
	return tree.balanceFactor()
}

func maybeBalance(tree *Tree) *Tree {
	var balanceFactor uint8 = balanceFactor(tree)
	if math.Abs(balanceFactor) <= 1 {
		return tree
	}

	var isLeft bool = balanceFactor < 0

	if isLeft == tree.link(isLeft).balanceFactor() > 0 {
		tree.walkExpect(isLeft, func (child *Tree) *Tree{ return rotate(child, isLeft) })
	}

	rotate(isLeft)
}

func rotate(tree *Tree, isLeft bool) (child *Tree) {
	child = tree.detachExpect(isLeft)
	var maybeGrandchild *Tree = child.detach(isLeft)

	err := tree.attach(isLeft, maybeGrandchild)
	if err != nil {
		fmt.Errorf("Fialed to attach grand child: %w", err)
	}
	tree = maybeBalance(tree)

	err := child.attach(!isLeft, tree)
	if err != nil {
		fmt.Errorf("Fialed to attach tree: %w", err)
	}

	return
}
