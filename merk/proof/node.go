package proof

import (
	m "github.com/tak1827/merk-go/merk"
)

type NodeType uint8

const (
	Hash NodeType = 1 << iota
	KVHash
	KV
)

type Node struct {
	t NodeType
	h m.Hash // for Hash, KVHash
	k []byte // for KV
	v []byte // for KV
}
