package merk

import (
	"bytes"
	"math/rand"
	"sort"
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

func BinarySearch(needle []byte, keys [][]byte) (bool, int) {
	low := 0
	high := len(keys) - 1

	for low <= high {
		median := (low + high) / 2

		if bytes.Compare(keys[median], needle) == 0 {
			return true, median
			// a < b
		} else if bytes.Compare(keys[median], needle) == -1 {
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

func RandIntn(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}
