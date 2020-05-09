package merk

import (
	"fmt"
	"math"
	// "sync"
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

func applyTo(maybeTree *Tree, batch Batch) (*Tree, [][]byte) {
	var deletedKeys [][]byte
	if batch != nil && maybeTree != nil {
		maybeTree, deletedKeys = apply(maybeTree, batch)
	} else if batch == nil {
		// Do nothing
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
	var midKey []byte = batch[midIndex].K
	var midOP OPType = batch[midIndex].O
	var midValue []byte = batch[midIndex].V
	if midOP == Del {
		panic(fmt.Sprintf("tried to delete non-existent key %v", midKey))
	}

	midTree := newTree(midKey, midValue)
	midTree, _ = recurse(midTree, batch, midIndex, true)

	return midTree
}

func apply(tree *Tree, batch Batch) (*Tree, [][]byte) {
	var (
		deletedKeys, deletedKeysRight [][]byte
		leftBatch, rightBatch         Batch
	)

	found, mid := binarySearchBy(tree.key(), batch)

	if found {
		switch batch[mid].O {
		case Put:
			tree.withValue(batch[mid].V)
		case Del:
			maybeTree := remove(tree)

			leftBatch = batch[:mid]
			rightBatch = batch[mid+1:]

			if len(leftBatch) != 0 {
				maybeTree, deletedKeys = applyTo(maybeTree, leftBatch)
			}
			if len(rightBatch) != 0 {
				maybeTree, deletedKeysRight = applyTo(maybeTree, rightBatch)
				deletedKeys = append(deletedKeys, deletedKeysRight...)
			}
			deletedKeys = append(deletedKeys, tree.key())

			return maybeTree, deletedKeys
		default:
			panic("Don't exist opcode")
		}
	}

	var exclusive bool = found

	return recurse(tree, batch, mid, exclusive)
}

func recurse(tree *Tree, batch Batch, mid int, exclusive bool) (*Tree, [][]byte) {
	var (
		leftBatch, rightBatch Batch
		deletedKeys           [][]byte
		// wg                    sync.WaitGroup
	)

	leftBatch = batch[:mid]
	if exclusive {
		rightBatch = batch[mid+1:]
	} else {
		rightBatch = batch[mid:]
	}

	if len(leftBatch) != 0 {
		// wg.Add(1)

		// go func() {
		// 	defer wg.Done()

		tree.walk(true, func(maybeLeft *Tree) *Tree {
			maybeLeft, deletedKeysLeft := applyTo(maybeLeft, leftBatch)

			deletedKeys = append(deletedKeys, deletedKeysLeft...)
			return maybeLeft
		})
		// }()
	}

	if len(rightBatch) != 0 {
		// wg.Add(1)

		// go func() {
		// 	defer wg.Done()

		tree.walk(false, func(maybeRight *Tree) *Tree {
			maybeRight, deletedKeysRight := applyTo(maybeRight, rightBatch)
			deletedKeys = append(deletedKeys, deletedKeysRight...)
			return maybeRight
		})
		// }()
	}

	// wg.Wait()

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
		tree.walkExpect(isLeft, func(child *Tree) *Tree { return rotate(child, !isLeft) })
	}

	return rotate(tree, isLeft)
}

func rotate(tree *Tree, isLeft bool) *Tree {
	var (
		err             error
		child           *Tree
		maybeGrandchild *Tree
	)

	child = tree.detachExpect(isLeft)
	maybeGrandchild = child.detach(!isLeft)

	if maybeGrandchild != nil {
		err = tree.attach(isLeft, maybeGrandchild)
		if err != nil {
			fmt.Errorf("fialed to attach grand child: %w", err)
		}
	}
	tree = maybeBalance(tree)

	err = child.attach(!isLeft, tree)
	if err != nil {
		fmt.Errorf("fialed to attach tree: %w", err)
	}
	child = maybeBalance(child)

	return child
}

func remove(tree *Tree) *Tree {
	var (
		hasLeft, hasRight, isLeft bool
		maybeTree                 *Tree
	)

	if tree.link(true) != nil {
		hasLeft = true
	}
	if tree.link(false) != nil {
		hasRight = true
	}
	isLeft = tree.childHeight(true) > tree.childHeight(false)

	if hasLeft && hasRight {
		// two children, promote edge of taller child
		tallChild := tree.detachExpect(isLeft)   // 88
		shortChild := tree.detachExpect(!isLeft) // 50
		maybeTree = promoteEdge(tallChild, shortChild, !isLeft)
	} else if hasLeft || hasRight {
		// single child, promote it
		maybeTree = tree.detachExpect(isLeft)
	} else {
		// no child
	}

	return maybeTree
}

func promoteEdge(tree, attach *Tree, isLeft bool) *Tree {
	var (
		edge, maybeChild *Tree
		err              error
	)

	edge, maybeChild = removeEdge(tree, isLeft)

	err = edge.attach(!isLeft, maybeChild)
	if err != nil {
		panic(err.Error())
	}

	err = edge.attach(isLeft, attach)
	if err != nil {
		panic(err.Error())
	}

	return maybeBalance(edge)
}

func removeEdge(t *Tree, isLeft bool) (*Tree, *Tree) {
	var tree, edge, maybeChild *Tree

	if t != nil && t.link(isLeft) != nil {
		child := t.detachExpect(isLeft)
		tree = t

		edge, maybeChild = removeEdge(child, isLeft)

		tree.attach(isLeft, maybeChild)
		tree = maybeBalance(tree)

		return edge, tree
	} else {
		tree = t.detach(!isLeft)
		return t, tree
	}
}

func sortBatch(b Batch) Batch {
	sort.SliceStable(b, func(i, j int) bool {
		return string(b[i].K) < string(b[j].K)
	})
	return b
}
