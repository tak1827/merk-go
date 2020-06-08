package merk

var _ Link = (*Modified)(nil)

type Modified struct {
	ch [2]uint8 // [left, right]
	t  *Tree
}

func (m *Modified) linkType() LinkType {
	return ModifiedLink
}

func (m *Modified) childHeights() [2]uint8 {
	return m.ch
}

func (m *Modified) tree() *Tree {
	return m.t
}

func (m *Modified) key() []byte {
	return m.t.Key()
}

func (m *Modified) Hash() Hash {
	return NullHash
}

func (m *Modified) height() uint8 {
	return 1 + max(m.ch[:])
}

func (m *Modified) balanceFactor() int8 {
	if m == nil {
		return 0
	}
	return int8(m.ch[1] - m.ch[0])
}

func (m *Modified) intoPruned() Link {
	panic("BUG: cannot prune Modified tree")
}

func (m *Modified) intoStored(tree *Tree) Link {
	panic("BUG: cannot restore from Modified tree")
}

func fromModifiedTree(tree *Tree) *Modified {
	return &Modified{
		ch: tree.childHeights(),
		t:  tree,
	}
}
