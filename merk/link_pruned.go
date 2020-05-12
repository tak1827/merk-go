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

func (p *Pruned) hash() Hash {
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
		ch: p.ch,
		t:  tree,
	}
}

// func (p *Pruned) marshal(buf *bytebufferpool.ByteBuffer) error {
// 	var buf64 [8]byte
// 	var key []byte = p.key()

// 	// Write key
// 	if uint32(len(key)) > uint32(math.MaxUint32) {
// 		return fmt.Errorf("too long, key: %v ", key)
// 	}
// 	binary.LittleEndian.PutUint32(buf64[:4], uint32(len(key)))
// 	if _, err := buf.Write(buf64[:4]); err != nil {
// 		return err
// 	}
// 	if _, err := buf.Write(key); err != nil {
// 		return err
// 	}

// 	// Write hash
// 	if _, err := buf.Write(p.h[:]); err != nil {
// 		return err
// 	}

// 	// Write child heights
// 	if err := buf.WriteByte(byte(p.ch[0])); err != nil {
// 		return err
// 	}
// 	if err := buf.WriteByte(byte(p.ch[1])); err != nil {
// 		return err
// 	}

// 	return nil
// }
