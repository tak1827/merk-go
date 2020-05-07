package merk

import (
	"encoding/binary"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"math"
)

type Stored struct {
	ch [2]uint8 // [left, right]
	t  *Tree
	h  Hash
}

func (s *Stored) linkType() LinkType {
	return StoredLink
}

func (s *Stored) childHeights() [2]uint8 {
	return s.ch
}

func (s *Stored) tree() *Tree {
	return s.t
}

func (s *Stored) key() []byte {
	return s.t.key()
}

func (s *Stored) hash() Hash {
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
	panic("BUG: cannot restore from Modified tree")
}

func (s *Stored) marshal(buf *bytebufferpool.ByteBuffer) error {
	var buf64 [8]byte
	var key []byte = s.key()

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
	if _, err := buf.Write(s.h[:]); err != nil {
		return err
	}

	// Write child heights
	if err := buf.WriteByte(byte(s.ch[0])); err != nil {
		return err
	}
	if err := buf.WriteByte(byte(s.ch[1])); err != nil {
		return err
	}

	return nil
}
