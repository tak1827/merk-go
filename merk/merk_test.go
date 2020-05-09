package merk

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
	"testing"
	// "github.com/davecgh/go-spew/spew"
)

const testDBName string = "testdb"

func TestApply(t *testing.T) {
	var batch1, batch2, batch3, batch4, batch5, batch6 Batch

	m, _ := newMerk()

	/** Insert & Update Case **/
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

	batch1 = append(batch1, op3, op6, op8)
	m.apply(batch1)

	batch2 = append(batch2, op0, op1, op2, op3, op6, op8)
	m.apply(batch2)

	batch3 = append(batch3, op4, op5, op7, op9)
	m.apply(batch3)

	require.NoError(t, m.tree.verify())

	/** Delete Case **/
	op10 := &OP{O: Del, K: []byte("0")}
	op11 := &OP{O: Del, K: []byte("1")}
	op12 := &OP{O: Del, K: []byte("2")}
	op13 := &OP{O: Del, K: []byte("3")}
	op14 := &OP{O: Del, K: []byte("4")}
	op15 := &OP{O: Del, K: []byte("5")}
	op16 := &OP{O: Del, K: []byte("6")}
	op17 := &OP{O: Del, K: []byte("7")}
	op18 := &OP{O: Del, K: []byte("8")}
	op19 := &OP{O: Del, K: []byte("9")}

	batch4 = append(batch4, op11, op15, op16, op19)
	delKeys4, _ := m.apply(batch4)

	require.EqualValues(t, [][]byte{[]byte("1"), []byte("5"), []byte("6"), []byte("9")}, delKeys4)
	require.NoError(t, m.tree.verify())

	batch5 = append(batch5, op12, op13, op17)
	delKeys5, _ := m.apply(batch5)

	require.EqualValues(t, [][]byte{[]byte("2"), []byte("3"), []byte("7")}, delKeys5)
	require.NoError(t, m.tree.verify())

	batch6 = append(batch6, op10, op14, op18)
	delKeys6, _ := m.apply(batch6)

	require.EqualValues(t, [][]byte{[]byte("0"), []byte("4"), []byte("8")}, delKeys6)
	require.Nil(t, m.tree)
}

func TestGet(t *testing.T) {
	var batch Batch

	m, _ := newMerk()

	op0 := &OP{Put, []byte("key0"), []byte("value0")}
	op1 := &OP{Put, []byte("key1"), []byte("value1")}
	op2 := &OP{Put, []byte("key2"), []byte("value2")}
	op3 := &OP{Put, []byte("key3"), []byte("value3")}
	op4 := &OP{Put, []byte("key4"), []byte("value4")}
	op5 := &OP{Put, []byte("key5"), []byte("value5")}
	op6 := &OP{Put, []byte("key6"), []byte("value6")}
	op7 := &OP{Put, []byte("key7"), []byte("value7")}
	op8 := &OP{Put, []byte("key8"), []byte("value8")}
	op9 := &OP{Put, []byte("key9"), []byte("value9")}

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
	require.EqualValues(t, PrunedLink, m.tree.child(true).child(true).link(true).linkType())
	require.EqualValues(t, PrunedLink, m.tree.child(true).child(false).link(true).linkType())
	require.EqualValues(t, PrunedLink, m.tree.child(false).child(true).link(true).linkType())
	require.Nil(t, m.tree.child(true).child(true).link(true).tree())
	require.Nil(t, m.tree.child(true).child(false).link(true).tree())
	require.Nil(t, m.tree.child(false).child(true).link(true).tree())
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

	op11 := &OP{O: Del, K: []byte("key1")}
	op15 := &OP{O: Del, K: []byte("key5")}
	op16 := &OP{O: Del, K: []byte("key6")}
	op19 := &OP{O: Del, K: []byte("key9")}

	batch = append(batch, op11, op15, op16, op19)
	delKeys, _ := m.apply(batch)

	require.EqualValues(t, [][]byte{[]byte("key1"), []byte("key5"), []byte("key6"), []byte("key9")}, delKeys)
	require.NoError(t, m.tree.verify())
}

func buildMerkWithDB() *Merk {
	var batch Batch

	m, _ := newMarkWithDB(testDBName)

	op0 := &OP{Put, []byte("key0"), []byte("value0")}
	op1 := &OP{Put, []byte("key1"), []byte("value1")}
	op2 := &OP{Put, []byte("key2"), []byte("value2")}
	op3 := &OP{Put, []byte("key3"), []byte("value3")}
	op4 := &OP{Put, []byte("key4"), []byte("value4")}
	op5 := &OP{Put, []byte("key5"), []byte("value5")}
	op6 := &OP{Put, []byte("key6"), []byte("value6")}
	op7 := &OP{Put, []byte("key7"), []byte("value7")}
	op8 := &OP{Put, []byte("key8"), []byte("value8")}
	op9 := &OP{Put, []byte("key9"), []byte("value9")}

	batch = append(batch, op0, op1, op2, op3, op4, op5, op6, op7, op8, op9)
	m.apply(batch)

	return m
}

func buildBatch(b Batch, size, n int) Batch {
	var batch Batch

	// create from ground
	if b == nil {
		for i := 0; i < size; i++ {
			key := blake2b.Sum256([]byte("key" + string(n) + string(i)))
			val := bytes.Repeat([]byte("x"), RandIntn(1000))
			op := &OP{Put, key[:], val}
			batch = append(batch, op)
		}

		return sortBatch(batch)
	}

	// update 1/2 and delete 1/20
	for i := 0; i < size/2; i++ {
		key1 := blake2b.Sum256([]byte("key" + string(n) + string(i)))
		val1 := bytes.Repeat([]byte("x"), RandIntn(1000))
		op1 := &OP{Put, key1[:], val1}

		if i%20 == 0 && b[i*2].O == Put {
			key2 := b[i*2].K
			op2 := &OP{O: Del, K: key2}

			batch = append(batch, op1, op2)
			continue
		}

		key2 := b[i*2].K
		val2 := bytes.Repeat([]byte("x"), RandIntn(1000))
		op2 := &OP{Put, key2[:], val2}
		batch = append(batch, op2, op1)
	}

	return sortBatch(batch)
}

func BenchmarkNoCommit(b *testing.B) {
	var (
		batch Batch
		size  int = 1000
	)

	m, _ := newMerk()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		batch = buildBatch(batch, size, n)
		if _, err := m.applyUnchecked(batch); err != nil {
			// if _, err := m.apply(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// func BenchmarkCommit(b *testing.B) {
// 	m, _ := newMerk()

// 	batchBuilder := func(n int) Batch {
// 		var (
// 			batch Batch
// 			keys  [][]byte
// 		)

// 		for i := 0; i < 100_000; i++ {
// 			key := blake2b.Sum256([]byte("key" + string(n) + string(i)))
// 			keys = append(keys, key[:])
// 		}

// 		sortBytes(keys)

// 		for _, key := range keys {
// 			value := bytes.Repeat([]byte("x"), RandIntn(1000))
// 			op := &OP{Put, key, value}
// 			batch = append(batch, op)
// 		}

// 		return batch
// 	}

// 	batch := batchBuilder(0)
// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	for n := 0; n < b.N; n++ {
// 		if _, err := m.applyUnchecked(batch); err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }
