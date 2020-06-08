package merk

var _ Link = (*Stored)(nil)

type Stored struct {
	ch [2]uint8 // [left, right]
	t  *Tree
	h  Hash
}

func (s *Stored) linkType() LinkType {
	return StoredLink
}

func (s *Stored) ChildHeights() [2]uint8 {
	return s.ch
}

func (s *Stored) tree() *Tree {
	return s.t
}

func (s *Stored) key() []byte {
	return s.t.Key()
}

func (s *Stored) Hash() Hash {
	return s.h
}

func (s *Stored) height() uint8 {
	return 1 + max(s.ch[:])
}

func (s *Stored) balanceFactor() int8 {
	if s == nil {
		return 0
	}
	return int8(s.ch[1] - s.ch[0])
}

func (s *Stored) intoPruned() Link {
	return &Pruned{
		ch: s.ch,
		k:  s.key(),
		h:  s.h,
	}
}

func (s *Stored) intoStored(tree *Tree) Link {
	return s
}
