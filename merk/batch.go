package merk

import (
	"sort"
)

type Batch []*OP

func sortBatch(b Batch) Batch {
	sort.SliceStable(b, func(i, j int) bool {
		return string(b[i].K) < string(b[j].K)
	})
	return b
}

func binarySearchBatch(needle []byte, batch Batch) (bool, int) {
	var keys [][]byte
	for _, op := range batch {
		keys = append(keys, op.K)
	}

	return BinarySearch(needle, keys)
}
