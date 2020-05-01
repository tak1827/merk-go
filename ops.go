package merk

import (
	"fmt"
	"math"
	"github.com/davecgh/go-spew/spew"
)

type OpType uint8

const (
	Put OpType = 1 << iota
	Delete
)

type Op struct {
	op  OpType
	key []byte
	val []byte
}

type Batch []*Op

func applyTo(maybeTree *Tree, batch Batch) (*Tree, [][]byte) {
	var deletedKeys [][]byte
	if batch != nil && maybeTree != nil {
		maybeTree, deletedKeys = apply(maybeTree, batch)
	} else {
		return build(batch), nil
	}

	return maybeTree, deletedKeys
}

func build(batch Batch) *Tree {
	if batch == nil {
		return nil
	}

	var midIndex int = len(batch) / 2
	var midKey []byte = batch[midIndex].key
	var midOp OpType = batch[midIndex].op
	var midValue []byte = batch[midIndex].val
	if midOp == Delete {
		panic(fmt.Sprintf("Tried to delete non-existent key %v", midKey))
	}

	midTree := newTree(midKey, midValue)
	midTree, _ = recurse(midTree, batch, midIndex, true)

	return midTree
}

func apply(tree *Tree, batch Batch) (*Tree, [][]byte) {
	found, mid := binarySearchBy(tree.key(), batch)

	if found {
		tree.withValue(batch[mid].val)
	}

	var exclusive bool = found

	return recurse(tree, batch, mid, exclusive)
}

func recurse(tree *Tree, batch Batch, mid int, exclusive bool) (*Tree, [][]byte) {
	var leftBatch Batch = batch[:mid]
	var rightBatch Batch
	if exclusive {
		rightBatch = batch[mid+1:]
	} else {
		rightBatch = batch[mid:]
	}

	var deletedKeys [][]byte

	if len(leftBatch) != 0 {
		tree.walk(true, func(maybeLeft *Tree) *Tree {
			maybeLeft, deletedKeysLeft := applyTo(maybeLeft, leftBatch)
	 		deletedKeys	= append(deletedKeys, deletedKeysLeft...)
	 		return maybeLeft
		})
	}

	if len(rightBatch) != 0 {
		tree.walk(false, func(maybeRight *Tree) *Tree {
			maybeRight, deletedKeysRight := applyTo(maybeRight, rightBatch)
	 		deletedKeys	= append(deletedKeys, deletedKeysRight...)
	 		return maybeRight
		})
	}

	return maybeBalance(tree), deletedKeys
}

func balanceFactor(tree *Tree) int8 {
	if tree == nil {
		return 0
	}
	return tree.balanceFactor()
}

func maybeBalance(tree *Tree) *Tree {
	var balance int8 = balanceFactor(tree)
	if math.Abs(float64(balance)) <= 1 {
		return tree
	}

	var isLeft bool = balance < 0
	var childIsLeft bool = balanceFactor(tree.child(isLeft)) > 0

	if (isLeft && childIsLeft) || (!isLeft && !childIsLeft) {
		spew.Dump("!")
		tree.walkExpect(isLeft, func (child *Tree) *Tree{ return rotate(child, !isLeft) })
	}

	return rotate(tree, isLeft)
}

func rotate(tree *Tree, isLeft bool) *Tree {
	var (
		err error
		child *Tree
		maybeGrandchild *Tree
	)

	child = tree.detachExpect(isLeft)
	maybeGrandchild = child.detach(!isLeft)

	if maybeGrandchild != nil {
		err = tree.attach(isLeft, maybeGrandchild)
		if err != nil {
			fmt.Errorf("Fialed to attach grand child: %w", err)
		}
	}
	tree = maybeBalance(tree)

	err = child.attach(!isLeft, tree)
	if err != nil {
		fmt.Errorf("Fialed to attach tree: %w", err)
	}
	child = maybeBalance(child)

	return child
}
