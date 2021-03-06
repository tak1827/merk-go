package merk

import (
	"fmt"
	"math"
)

type OPType uint8

const (
	Put OPType = 1 << iota
	Del
)

type OP struct {
	O OPType
	K []byte
	V []byte
}

func applyTo(maybeTree *Tree, batch Batch) (*Tree, [][]byte, error) {
	if maybeTree == nil {
		t, err := build(batch)
		return t, nil, err
	}

	return apply(maybeTree, batch)
}

func build(batch Batch) (*Tree, error) {
	var midIndex int = len(batch) / 2
	var midKey []byte = batch[midIndex].K
	var midOP OPType = batch[midIndex].O
	var midValue []byte = batch[midIndex].V
	if midOP == Del {
		return nil, fmt.Errorf("tried to delete non-existent key %v", midKey)
	}

	midTree := newTree(midKey, midValue)
	midTree, _, err := recurse(midTree, batch, midIndex, true)

	return midTree, err
}

func apply(tree *Tree, batch Batch) (*Tree, [][]byte, error) {
	var (
		deletedKeys, deletedKeysRight [][]byte
		leftBatch, rightBatch         Batch
		err                           error
	)

	found, mid := binarySearchBatch(tree.Key(), batch)

	if found {
		switch batch[mid].O {
		case Del:
			maybeTree := remove(tree)

			leftBatch = batch[:mid]
			rightBatch = batch[mid+1:]

			if len(leftBatch) != 0 {
				maybeTree, deletedKeys, err = applyTo(maybeTree, leftBatch)
				if err != nil {
					return nil, nil, err
				}
			}

			if len(rightBatch) != 0 {
				maybeTree, deletedKeysRight, err = applyTo(maybeTree, rightBatch)
				if err != nil {
					return nil, nil, err
				}
			}

			deletedKeys = append(deletedKeys, deletedKeysRight...)
			deletedKeys = append(deletedKeys, tree.Key())

			return maybeTree, deletedKeys, nil
		default:
			tree.withValue(batch[mid].V)
		}
	}

	return recurse(tree, batch, mid, found)
}

func recurse(tree *Tree, batch Batch, mid int, exclusive bool) (*Tree, [][]byte, error) {
	var (
		leftBatch, rightBatch             Batch
		deletedKeysLeft, deletedKeysRight [][]byte
	)

	leftBatch = batch[:mid]
	if exclusive {
		rightBatch = batch[mid+1:]
	} else {
		rightBatch = batch[mid:]
	}

	// Note: if use concurency, slow down when low spec pc
	chErr := make(chan error, 2)

	handler := func(isLeft bool, b Batch) {
		if len(b) != 0 {
			go func() {

				err := tree.walk(isLeft, func(maybeTree *Tree) (*Tree, error) {
					maybeTree, deletedKeys, err := applyTo(maybeTree, b)
					if isLeft {
						deletedKeysLeft = append(deletedKeysLeft, deletedKeys...)
					} else {
						deletedKeysRight = append(deletedKeysRight, deletedKeys...)
					}
					return maybeTree, err
				})
				chErr <- err
			}()
		} else {
			chErr <- nil
		}
	}

	handler(true, leftBatch)
	handler(false, rightBatch)

	for i := 0; i < cap(chErr); i++ {
		if err := <-chErr; err != nil {
			return nil, nil, err
		}
	}

	return maybeBalance(tree), append(deletedKeysLeft, deletedKeysRight...), nil
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
	var childIsLeft bool = balanceFactor(tree.Child(isLeft)) > 0

	if (isLeft && childIsLeft) || (!isLeft && !childIsLeft) {
		tree.walkExpect(isLeft, func(child *Tree) *Tree { return rotate(child, !isLeft) })
	}

	return rotate(tree, isLeft)
}

func rotate(tree *Tree, isLeft bool) *Tree {
	var (
		child           *Tree
		maybeGrandchild *Tree
	)

	child = tree.detachExpect(isLeft)
	maybeGrandchild = child.detach(!isLeft)

	if maybeGrandchild != nil {
		tree.attach(isLeft, maybeGrandchild)
	}
	tree = maybeBalance(tree)

	child.attach(!isLeft, tree)
	child = maybeBalance(child)

	return child
}

func remove(tree *Tree) *Tree {
	var hasLeft, hasRight, isLeft bool

	if tree.Link(true) != nil {
		hasLeft = true
	}

	if tree.Link(false) != nil {
		hasRight = true
	}

	// no child
	if !hasLeft && !hasRight {
		return nil
	}

	isLeft = tree.ChildHeight(true) > tree.ChildHeight(false)

	// single child
	if !(hasLeft && hasRight) {
		return tree.detachExpect(isLeft)
	}

	// two children, promote edge of taller child
	tallChild := tree.detachExpect(isLeft)
	shortChild := tree.detachExpect(!isLeft)
	return promoteEdge(tallChild, shortChild, !isLeft)
}

func promoteEdge(tree, attach *Tree, isLeft bool) *Tree {
	var edge, maybeChild *Tree

	edge, maybeChild = removeEdge(tree, isLeft)

	edge.attach(!isLeft, maybeChild)
	edge.attach(isLeft, attach)

	return maybeBalance(edge)
}

func removeEdge(t *Tree, isLeft bool) (*Tree, *Tree) {
	var tree, edge, maybeChild *Tree

	if t.Link(isLeft) == nil {
		tree = t.detach(!isLeft)
		return t, tree
	}

	child := t.detachExpect(isLeft)
	tree = t

	edge, maybeChild = removeEdge(child, isLeft)

	tree.attach(isLeft, maybeChild)
	tree = maybeBalance(tree)

	return edge, tree
}
