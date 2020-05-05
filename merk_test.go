package merk

import (
	"testing"
	"github.com/stretchr/testify/require"
	// "errors"
	// "github.com/davecgh/go-spew/spew"
)

func TestApply(t *testing.T) {
	var batch1, batch2, batch3, batch4, batch5, batch6 Batch

	m, _ := newMerk("")
	defer closeDB(m.db)

	/** Insert & Update Case **/
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

	batch2 = append(batch2, op0, op1, op2, op3, op6, op8)
	m.apply(batch2)

	batch3 = append(batch3, op4, op5, op7, op9)
	m.apply(batch3)

	require.EqualValues(t, []byte("3"), m.tree.key())
	require.EqualValues(t, []byte("1"), m.tree.child(true).key())
	require.EqualValues(t, []byte("0"), m.tree.child(true).child(true).key())
	require.EqualValues(t, []byte("2"), m.tree.child(true).child(false).key())
	require.EqualValues(t, []byte("6"), m.tree.child(false).key())
	require.EqualValues(t, []byte("5"), m.tree.child(false).child(true).key())
	require.EqualValues(t, []byte("4"), m.tree.child(false).child(true).child(true).key())
	require.EqualValues(t, []byte("8"), m.tree.child(false).child(false).key())
	require.EqualValues(t, []byte("7"), m.tree.child(false).child(false).child(true).key())
	require.EqualValues(t, []byte("9"), m.tree.child(false).child(false).child(false).key())


	/** Delete Case **/
	op10 := &Op{op:Delete, key:[]byte("0")}
	op11 := &Op{op:Delete, key:[]byte("1")}
	op12 := &Op{op:Delete, key:[]byte("2")}
	op13 := &Op{op:Delete, key:[]byte("3")}
	op14 := &Op{op:Delete, key:[]byte("4")}
	op15 := &Op{op:Delete, key:[]byte("5")}
	op16 := &Op{op:Delete, key:[]byte("6")}
	op17 := &Op{op:Delete, key:[]byte("7")}
	op18 := &Op{op:Delete, key:[]byte("8")}
	op19 := &Op{op:Delete, key:[]byte("9")}

	batch4 = append(batch4, op11, op15, op16, op19)
	m.apply(batch4)

	require.EqualValues(t, []byte("3"), m.tree.key())
	require.EqualValues(t, []byte("2"), m.tree.child(true).key())
	require.EqualValues(t, []byte("0"), m.tree.child(true).child(true).key())
	require.EqualValues(t, []byte("7"), m.tree.child(false).key())
	require.EqualValues(t, []byte("4"), m.tree.child(false).child(true).key())
	require.EqualValues(t, []byte("8"), m.tree.child(false).child(false).key())

	batch5 = append(batch5, op12, op13, op17)
	m.apply(batch5)

	require.EqualValues(t, []byte("4"), m.tree.key())
	require.EqualValues(t, []byte("0"), m.tree.child(true).key())
	require.EqualValues(t, []byte("8"), m.tree.child(false).key())

	batch6 = append(batch6, op10, op14, op18)
	m.apply(batch6)

	require.Nil(t, m.tree)

	// require.NoError(t, errors.New("Debug"))
}

func TestGet(t *testing.T) {
	var batch1 Batch

	m, _ := newMerk("")
	defer closeDB(m.db)

	/** Insert & Update Case **/
	op0 := &Op{Put, []byte("0"), []byte("value0")}
	op1 := &Op{Put, []byte("1"), []byte("value1")}
	op2 := &Op{Put, []byte("2"), []byte("value2")}
	op3 := &Op{Put, []byte("3"), []byte("value3")}
	op4 := &Op{Put, []byte("4"), []byte("value4")}
	op5 := &Op{Put, []byte("5"), []byte("value5")}
	op6 := &Op{Put, []byte("6"), []byte("value6")}
	op7 := &Op{Put, []byte("7"), []byte("value7")}
	op8 := &Op{Put, []byte("8"), []byte("value8")}
	op9 := &Op{Put, []byte("9"), []byte("value9")}

	batch1 = append(batch1, op0, op1, op2, op3, op4, op5, op6, op7, op8, op9)
	m.apply(batch1)

	require.EqualValues(t, []byte("value0"), m.get([]byte("0")))
	require.EqualValues(t, []byte("value1"), m.get([]byte("1")))
	require.EqualValues(t, []byte("value2"), m.get([]byte("2")))
	require.EqualValues(t, []byte("value3"), m.get([]byte("3")))
	require.EqualValues(t, []byte("value4"), m.get([]byte("4")))
	require.EqualValues(t, []byte("value5"), m.get([]byte("5")))
	require.EqualValues(t, []byte("value6"), m.get([]byte("6")))
	require.EqualValues(t, []byte("value7"), m.get([]byte("7")))
	require.EqualValues(t, []byte("value8"), m.get([]byte("8")))
	require.EqualValues(t, []byte("value9"), m.get([]byte("9")))
}
