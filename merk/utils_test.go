package merk

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMax(t *testing.T) {
	data1 := []uint8{0, 1, 2, 3, 4, 5}

	require.EqualValues(t, max(data1), 5)
}

func TestBinarySearch(t *testing.T) {
	var (
		isFound bool
		index   int
	)

	key0 := []byte("0")
	key1 := []byte("1")
	key2 := []byte("2")
	key3 := []byte("3")
	key4 := []byte("4")
	key5 := []byte("5")
	key6 := []byte("6")
	key7 := []byte("7")
	key8 := []byte("8")
	key9 := []byte("9")

	var keys [][]byte
	keys = append(keys, key1, key2, key4, key5, key6, key8)

	isFound, index = BinarySearch(key4, keys)
	require.True(t, isFound)
	require.EqualValues(t, index, 2)

	isFound, index = BinarySearch(key8, keys)
	require.True(t, isFound)
	require.EqualValues(t, index, 5)

	isFound, index = BinarySearch(key3, keys)
	require.True(t, !isFound)
	require.EqualValues(t, index, 2)

	isFound, index = BinarySearch(key7, keys)
	require.True(t, !isFound)
	require.EqualValues(t, index, 5)

	isFound, index = BinarySearch(key0, keys)
	require.True(t, !isFound)
	require.EqualValues(t, index, 0)

	isFound, index = BinarySearch(key9, keys)
	require.True(t, !isFound)
	require.EqualValues(t, index, 6)
}

func TestSerializeBytes(t *testing.T) {
	byte1 := []byte("1")
	byte2 := []byte("2")
	byte3 := []byte("3")

	require.EqualValues(t, serializeBytes(byte1, byte2, byte3), []byte("123"))
}

func TestSortBytes(t *testing.T) {
	b := [][]byte{[]byte("3"), []byte("2"), []byte("1"), []byte("4")}

	sortBytes(b)

	require.EqualValues(t, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, b)
}

func TestRandIntn(t *testing.T) {
	require.True(t, RandIntn(10) < 10)
}
