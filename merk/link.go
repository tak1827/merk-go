package merk

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
	Hash() Hash

	height() uint8
	balanceFactor() int8

	intoPruned() Link
	intoStored(tree *Tree) Link
}
