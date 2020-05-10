package merk

import (
	"fmt"
	"math"
	"sync"
	"sort"
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

type Batch []*OP

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
		err error
	)

	found, mid := binarySearchBy(tree.key(), batch)

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
			deletedKeys = append(deletedKeys, tree.key())

			return maybeTree, deletedKeys, nil
		default:
			tree.withValue(batch[mid].V)
		}
	}

	return recurse(tree, batch, mid, found)
}

func recurse(tree *Tree, batch Batch, mid int, exclusive bool) (*Tree, [][]byte, error) {
	var (
		leftBatch, rightBatch Batch
		deletedKeys           [][]byte
		wg                    sync.WaitGroup
	)

	leftBatch = batch[:mid]
	if exclusive {
		rightBatch = batch[mid+1:]
	} else {
		rightBatch = batch[mid:]
	}

	if len(leftBatch) != 0 {
		wg.Add(1)

		go func() {
			defer wg.Done()

		if err := tree.walk(true, func(maybeLeft *Tree) (*Tree, error) {
			maybeLeft, deletedKeysLeft, err  := applyTo(maybeLeft, leftBatch)
			deletedKeys = append(deletedKeys, deletedKeysLeft...)
			return maybeLeft, err
		}); err != nil {
			fmt.Errorf("error while concurency, %w", err)
			// return nil, nil, err
		}
		}()
	}

	if len(rightBatch) != 0 {
		wg.Add(1)

		go func() {
			defer wg.Done()

		if err := tree.walk(false, func(maybeRight *Tree) (*Tree, error) {
			maybeRight, deletedKeysRight, err := applyTo(maybeRight, rightBatch)
			deletedKeys = append(deletedKeys, deletedKeysRight...)
			return maybeRight, err
		}); err != nil {
			fmt.Errorf("error while concurency, %w", err)
			// return nil, nil, err
		}
		}()
	}

	wg.Wait()

	return maybeBalance(tree), deletedKeys, nil
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

	if tree.link(true) != nil {
		hasLeft = true
	}

	if tree.link(false) != nil {
		hasRight = true
	}

	// no child
  if !hasLeft && !hasRight {
    return nil
  }

	isLeft = tree.childHeight(true) > tree.childHeight(false)

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

	if t.link(isLeft) == nil {
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

func sortBatch(b Batch) Batch {
	sort.SliceStable(b, func(i, j int) bool {
		return string(b[i].K) < string(b[j].K)
	})
	return b
}
