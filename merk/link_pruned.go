package merk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"math"
)

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

func (p *Pruned) marshal(buf *bytebufferpool.ByteBuffer) error {
	var buf64 [8]byte
	var key []byte = p.key()

	// Write key
	if uint32(len(key)) > uint32(math.MaxUint32) {
		return fmt.Errorf("too long, key: %v ", key)
	}
	binary.LittleEndian.PutUint32(buf64[:4], uint32(len(key)))
	if _, err := buf.Write(buf64[:4]); err != nil {
		return err
	}
	if _, err := buf.Write(key); err != nil {
		return err
	}

	// Write hash
	if _, err := buf.Write(p.h[:]); err != nil {
		return err
	}

	// Write child heights
	if err := buf.WriteByte(byte(p.ch[0])); err != nil {
		return err
	}
	if err := buf.WriteByte(byte(p.ch[1])); err != nil {
		return err
	}

	return nil
}

func unmarshalPruned(r *bytes.Reader) (*Pruned, error) {
	var buf64 [8]byte

	p := new(Pruned)

	// Read key
	if _, err := r.Read(buf64[:4]); err != nil {
		return nil, err
	}
	p.k = make([]byte, binary.LittleEndian.Uint32(buf64[:4]))
	if _, err := r.Read(p.k); err != nil {
		return nil, err
	}

	// Read hash
	if _, err := r.Read(p.h[:]); err != nil {
		return nil, err
	}

	// Read left child height
	left, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.ch[0] = uint8(left)

	// Read right child height
	right, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	p.ch[1] = uint8(right)

	return p, nil
}
