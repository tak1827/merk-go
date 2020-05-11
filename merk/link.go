package merk

import (
	"github.com/valyala/bytebufferpool"
)

type LinkType uint8

const (
	PrunedLink LinkType = iota + 1
	ModifiedLink
	StoredLink
)

type Link interface {
	linkType() LinkType

	childHeights() [2]uint8
	tree() *Tree
	key() []byte
	hash() Hash

	height() uint8
	balanceFactor() int8

	intoPruned() Link
	intoStored(tree *Tree) Link

	marshal(buf *bytebufferpool.ByteBuffer) error
}
