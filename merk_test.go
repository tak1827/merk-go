package merk

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestApply(t *testing.T) {
	var (
		batch1 Batch
		batch2 Batch
		batch3 Batch
	)

	m := newMerk()

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

	batch1 = append(batch1, op3, op6, op8)
	m.apply(batch1)

	batch2 = append(batch2, op0, op1, op2)
	m.apply(batch2)

	batch3 = append(batch3, op4, op5, op7, op9)
	m.apply(batch3)

	assert.EqualValues(t, []byte("3"), m.tree.key())
	assert.EqualValues(t, []byte("1"), m.tree.child(true).key())
	assert.EqualValues(t, []byte("0"), m.tree.child(true).child(true).key())
	assert.EqualValues(t, []byte("2"), m.tree.child(true).child(false).key())
	assert.EqualValues(t, []byte("6"), m.tree.child(false).key())
	assert.EqualValues(t, []byte("5"), m.tree.child(false).child(true).key())
	assert.EqualValues(t, []byte("4"), m.tree.child(false).child(true).child(true).key())
	assert.EqualValues(t, []byte("8"), m.tree.child(false).child(false).key())
	assert.EqualValues(t, []byte("7"), m.tree.child(false).child(false).child(true).key())
	assert.EqualValues(t, []byte("9"), m.tree.child(false).child(false).child(false).key())
}
