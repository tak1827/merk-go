package merk

import (
	"errors"
)

type LinkType uint8

const (
	Pruned LinkType = 1 << iota
	Modified
	Stored
)

type Link struct {
	linkType LinkType
	// hash
	key []byte
	childHeights [2]uint8 // [left, right]
	pendingWrites uint8
	tree *Tree
}

func fromModifiedTree(maybeTree *Tree) *Link {
	var pending_writes uint8 = 1
		+ maybeTree.childPendingWrites(true)
		+ maybeTree.childPendingWrites(false)

	return &Link{
		linkType: Modified,
		childHeights: t.childHeights,
		tree: maybeTree,
	}
	
}

// func maybeFromModifiedTree(maybeTree *Tree) *Link {
// 	return fromModifiedTree(maybeTree)
// }

func (l *Link) isPruned() bool {
	return l.linkType == Pruned
}

func (l *Link) isModified() bool {
	return l.linkType == Modified
}

func (l *Link) isStored() bool {
	return l.linkType == Stored
}

func (l *Link) key() []byte {
	switch l.linkType {
	case Pruned:
		return l.key
	case Modified || Stored:
		return l.tree.key()
	default:
		panic("link type dose not match")
	}
}

func (l *Link) tree() *tree {
	switch l.linkType {
	case Pruned:
		return nil
	case Modified || Stored:
		return l.tree
	default:
		panic("link type dose not match")
	}
}

func (l *Link) height() uint8 {
	return 1+ max(l.childHeights[:])
}

func (l *Link) balanceFactor() int8 {
	return int8(l.childHeights[1]) - l.childHeights[0])
}


func (l *Link) intoPruned() {
	switch l.linkType {
	case Pruned:
		return
	case Modified:
		panic("Cannot prune Modified tree")
	case Stored:
		l.linkType = Pruned
		l.key = l.tree.key()
		l.tree = nil
		return
	default:
		panic("link type dose not match")
	}
}
