package merk

var _ Link = (*Pruned)(nil)

type Pruned struct {
	ch [2]uint8 // [left, right]
	k  []byte   // this is key of db
	h  Hash
}

func (p *Pruned) linkType() LinkType {
	return PrunedLink
}

func (p *Pruned) childHeights() [2]uint8 {
	return p.ch
}

func (p *Pruned) tree() *Tree {
	return nil
}

func (p *Pruned) key() []byte {
	return p.k
}

func (p *Pruned) Hash() Hash {
	return p.h
}

func (p *Pruned) height() uint8 {
	return 1 + max(p.ch[:])
}

func (p *Pruned) balanceFactor() int8 {
	if p == nil {
		return 0
	}
	return int8(p.ch[1] - p.ch[0])
}

func (p *Pruned) intoPruned() Link {
	return p
}

func (p *Pruned) intoStored(tree *Tree) Link {
	return &Stored{
		ch: tree.childHeights(),
		t:  tree,
		h:  p.h,
	}
}
