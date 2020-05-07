package merk

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMax(t *testing.T) {
	data1 := []uint8{0, 1, 2, 3, 4, 5}

	assert.EqualValues(t, max(data1), 5)
}

func TestBinarySearchBy(t *testing.T) {
	var (
		isFound bool
		index   int
	)

	op0 := &Op{Put, []byte("0"), []byte("value")}
	op1 := &Op{Put, []byte("1"), []byte("value")}
	op2 := &Op{Put, []byte("2"), []byte("value")}
	op3 := &Op{Put, []byte("3"), []byte("value")}
	op4 := &Op{Put, []byte("4"), []byte("value")}
	op5 := &Op{Put, []byte("5"), []byte("value")}
	op6 := &Op{Put, []byte("6"), []byte("value")}
	op7 := &Op{Put, []byte("7"), []byte("value")}
	op8 := &Op{Put, []byte("8"), []byte("value")}
	op9 := &Op{Put, []byte("9"), []byte("value")}

	var batch1 Batch
	batch1 = append(batch1, op1, op2, op4, op5, op6, op8)

	isFound, index = binarySearchBy(op4.key, batch1)
	assert.True(t, isFound)
	assert.EqualValues(t, index, 2)

	isFound, index = binarySearchBy(op8.key, batch1)
	assert.True(t, isFound)
	assert.EqualValues(t, index, 5)

	isFound, index = binarySearchBy(op3.key, batch1)
	assert.True(t, !isFound)
	assert.EqualValues(t, index, 2)

	isFound, index = binarySearchBy(op7.key, batch1)
	assert.True(t, !isFound)
	assert.EqualValues(t, index, 5)

	isFound, index = binarySearchBy(op0.key, batch1)
	assert.True(t, !isFound)
	assert.EqualValues(t, index, 0)

	isFound, index = binarySearchBy(op9.key, batch1)
	assert.True(t, !isFound)
	assert.EqualValues(t, index, 6)
}

func TestSerializeBytes(t *testing.T) {
	byte1 := []byte("1")
	byte2 := []byte("2")
	byte3 := []byte("3")

	assert.EqualValues(t, serializeBytes(byte1, byte2, byte3), []byte("123"))
}
