package merk

type LinkType uint8

const (
	Pruned LinkType = 1 << iota
	Modified
	Stored
)

type Link struct {
	linkType      LinkType
	hash          Hash
	key           []byte
	childHeights  [2]uint8 // [left, right]
	pendingWrites uint8
	tree          *Tree
}

func fromModifiedTree(tree *Tree) *Link {
	var pendingWrites uint8 = 1 +
		tree.childPendingWrites(true) +
		tree.childPendingWrites(false)

	return &Link{
		linkType: Modified,
		pendingWrites: pendingWrites,
		childHeights: tree.childHeights(),
		tree: tree,
	}
}

func (l *Link) isPruned() bool {
	return l.linkType == Pruned
}

func (l *Link) isModified() bool {
	return l.linkType == Modified
}

func (l *Link) isStored() bool {
	return l.linkType == Stored
}

// func (l *Link) hash() Hash {
// 	switch l.linkType {
// 	case Modified:
// 		panic("Cannot get hash from modified link")
// 	case Pruned:
// 		return l.hash
// 	case Stored:
// 		return l.hash
// 	default:
// 		panic("link type dose not match")
// 	}
// }

func (l *Link) height() uint8 {
	return 1 + max(l.childHeights[:])
}

func (l *Link) balanceFactor() int8 {
	if l == nil {
		return 0
	}
	return int8(l.childHeights[1] - l.childHeights[0])
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
