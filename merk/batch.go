package merk

import (
	"sort"
)

type OPType uint8

const (
	Put OPType = 1 << iota
	Del
)

type OP struct {
	O OPType
	K []byte
	V []byte
}

type Batch []*OP

func sortBatch(b Batch) Batch {
	sort.SliceStable(b, func(i, j int) bool {
		return string(b[i].K) < string(b[j].K)
	})
	return b
}
