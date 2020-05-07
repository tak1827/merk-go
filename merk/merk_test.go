package merk

import (
	"github.com/stretchr/testify/require"
	"testing"
)

const testDBName string = "testdb"

func TestApply(t *testing.T) {
	var batch1, batch2, batch3, batch4, batch5, batch6 Batch

	m, _ := newMerk()

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

	require.NoError(t, m.tree.verify())

	/** Delete Case **/
	op10 := &Op{op: Delete, key: []byte("0")}
	op11 := &Op{op: Delete, key: []byte("1")}
	op12 := &Op{op: Delete, key: []byte("2")}
	op13 := &Op{op: Delete, key: []byte("3")}
	op14 := &Op{op: Delete, key: []byte("4")}
	op15 := &Op{op: Delete, key: []byte("5")}
	op16 := &Op{op: Delete, key: []byte("6")}
	op17 := &Op{op: Delete, key: []byte("7")}
	op18 := &Op{op: Delete, key: []byte("8")}
	op19 := &Op{op: Delete, key: []byte("9")}

	batch4 = append(batch4, op11, op15, op16, op19)
	delKeys4 := m.apply(batch4)

	require.EqualValues(t, [][]byte{[]byte("1"), []byte("5"), []byte("6"), []byte("9")}, delKeys4)
	require.NoError(t, m.tree.verify())

	batch5 = append(batch5, op12, op13, op17)
	delKeys5 := m.apply(batch5)

	require.EqualValues(t, [][]byte{[]byte("2"), []byte("3"), []byte("7")}, delKeys5)
	require.NoError(t, m.tree.verify())

	batch6 = append(batch6, op10, op14, op18)
	delKeys6 := m.apply(batch6)

	require.EqualValues(t, [][]byte{[]byte("0"), []byte("4"), []byte("8")}, delKeys6)
	require.Nil(t, m.tree)
}

func TestGet(t *testing.T) {
	var batch Batch

	m, _ := newMerk()

	op0 := &Op{Put, []byte("key0"), []byte("value0")}
	op1 := &Op{Put, []byte("key1"), []byte("value1")}
	op2 := &Op{Put, []byte("key2"), []byte("value2")}
	op3 := &Op{Put, []byte("key3"), []byte("value3")}
	op4 := &Op{Put, []byte("key4"), []byte("value4")}
	op5 := &Op{Put, []byte("key5"), []byte("value5")}
	op6 := &Op{Put, []byte("key6"), []byte("value6")}
	op7 := &Op{Put, []byte("key7"), []byte("value7")}
	op8 := &Op{Put, []byte("key8"), []byte("value8")}
	op9 := &Op{Put, []byte("key9"), []byte("value9")}

	batch = append(batch, op0, op1, op2, op3, op4, op5, op6, op7, op8, op9)
	m.apply(batch)

	require.EqualValues(t, []byte("value0"), m.get([]byte("key0")))
	require.EqualValues(t, []byte("value1"), m.get([]byte("key1")))
	require.EqualValues(t, []byte("value2"), m.get([]byte("key2")))
	require.EqualValues(t, []byte("value3"), m.get([]byte("key3")))
	require.EqualValues(t, []byte("value4"), m.get([]byte("key4")))
	require.EqualValues(t, []byte("value5"), m.get([]byte("key5")))
	require.EqualValues(t, []byte("value6"), m.get([]byte("key6")))
	require.EqualValues(t, []byte("value7"), m.get([]byte("key7")))
	require.EqualValues(t, []byte("value8"), m.get([]byte("key8")))
	require.EqualValues(t, []byte("value9"), m.get([]byte("key9")))
}

func TestCommit(t *testing.T) {
	m := buildMerkWithDB()

	defer gDB.closeDB()
	defer gDB.destroy()

	require.NoError(t, m.tree.verify())
	require.Nil(t, m.tree.child(true).child(true).link(true).tree)
	require.Nil(t, m.tree.child(true).child(false).link(true).tree)
	require.Nil(t, m.tree.child(false).child(true).link(true).tree)
}

func TestCommitFetchTree(t *testing.T) {
	m := buildMerkWithDB()

	gDB.closeDB()

	m, _ = newMarkWithDB(testDBName)
	defer gDB.closeDB()
	defer gDB.destroy()

	require.NoError(t, m.tree.verify())
}

func TestCommitDel(t *testing.T) {
	var batch Batch
	m := buildMerkWithDB()
	defer gDB.closeDB()
	defer gDB.destroy()

	op11 := &Op{op: Delete, key: []byte("key1")}
	op15 := &Op{op: Delete, key: []byte("key5")}
	op16 := &Op{op: Delete, key: []byte("key6")}
	op19 := &Op{op: Delete, key: []byte("key9")}

	batch = append(batch, op11, op15, op16, op19)
	delKeys := m.apply(batch)

	require.EqualValues(t, [][]byte{[]byte("key1"), []byte("key5"), []byte("key6"), []byte("key9")}, delKeys)
	require.NoError(t, m.tree.verify())
}

func buildMerkWithDB() *Merk {
	var batch Batch

	m, _ := newMarkWithDB(testDBName)

	op0 := &Op{Put, []byte("key0"), []byte("value0")}
	op1 := &Op{Put, []byte("key1"), []byte("value1")}
	op2 := &Op{Put, []byte("key2"), []byte("value2")}
	op3 := &Op{Put, []byte("key3"), []byte("value3")}
	op4 := &Op{Put, []byte("key4"), []byte("value4")}
	op5 := &Op{Put, []byte("key5"), []byte("value5")}
	op6 := &Op{Put, []byte("key6"), []byte("value6")}
	op7 := &Op{Put, []byte("key7"), []byte("value7")}
	op8 := &Op{Put, []byte("key8"), []byte("value8")}
	op9 := &Op{Put, []byte("key9"), []byte("value9")}

	batch = append(batch, op0, op1, op2, op3, op4, op5, op6, op7, op8, op9)
	m.apply(batch)

	return m
}
