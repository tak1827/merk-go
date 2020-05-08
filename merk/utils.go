package merk

import (
	"bytes"
	"sort"
	"math/rand"
    "time"
)

func max(a []uint8) uint8 {
	max := a[0]
	for _, i := range a {
		if i > max {
			max = i
		}
	}

	return max
}

func binarySearchBy(needle []byte, batch Batch) (bool, int) {
	low := 0
	high := len(batch) - 1

	for low <= high {
		median := (low + high) / 2

		if bytes.Compare(batch[median].key, needle) == 0 {
			return true, median
			// a < b
		} else if bytes.Compare(batch[median].key, needle) == -1 {
			low = median + 1
		} else {
			high = median - 1
		}
	}

	return false, low
}

func serializeBytes(bs ...[]byte) (result []byte) {
	for _, b := range bs {
		result = append(result, b...)
	}

	return
}

func sortBytes(data [][]byte) {
	sort.SliceStable(data, func(i, j int) bool {
		return string(data[i]) < string(data[j])
	})
}

func randIntn(max int) int {
	rand.Seed(time.Now().UnixNano())
  return rand.Intn(max)
}
