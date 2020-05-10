package merk

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuild(t *testing.T) {
	var b Batch

	op0 := &OP{Put, []byte("0"), []byte("value")}
	op1 := &OP{Put, []byte("1"), []byte("value")}
	op2 := &OP{Put, []byte("2"), []byte("value")}
	op3 := &OP{Put, []byte("3"), []byte("value")}
	op4 := &OP{Put, []byte("4"), []byte("value")}
	op5 := &OP{Put, []byte("5"), []byte("value")}
	op6 := &OP{Put, []byte("6"), []byte("value")}
	op7 := &OP{Put, []byte("7"), []byte("value")}
	op8 := &OP{Put, []byte("8"), []byte("value")}
	op9 := &OP{Put, []byte("9"), []byte("value")}

	b = append(b, op0, op1, op2, op3, op4, op5, op6, op7, op8, op9)

	tree, _ := build(b)

	assert.EqualValues(t, []byte("5"), tree.key())
	assert.EqualValues(t, []byte("2"), tree.child(true).key())
	assert.EqualValues(t, []byte("1"), tree.child(true).child(true).key())
	assert.EqualValues(t, []byte("0"), tree.child(true).child(true).child(true).key())
	assert.EqualValues(t, []byte("4"), tree.child(true).child(false).key())
	assert.EqualValues(t, []byte("3"), tree.child(true).child(false).child(true).key())
	assert.EqualValues(t, []byte("8"), tree.child(false).key())
	assert.EqualValues(t, []byte("7"), tree.child(false).child(true).key())
	assert.EqualValues(t, []byte("6"), tree.child(false).child(true).child(true).key())
	assert.EqualValues(t, []byte("9"), tree.child(false).child(false).key())
}

func TestSortBatch(t *testing.T) {
	var b, expect Batch

	op0 := &OP{Put, []byte("key0"), []byte("value0")}
	op1 := &OP{Put, []byte("key1"), []byte("value1")}
	op2 := &OP{Del, []byte("key2"), []byte("")}
	op3 := &OP{Put, []byte("key3"), []byte("value3")}
	op4 := &OP{Put, []byte("key4"), []byte("")}

	b = append(b, op4, op1, op2, op0, op3)
	expect = append(expect, op0, op1, op2, op3, op4)

	b = sortBatch(b)

	assert.EqualValues(t, expect, b)
}
