package merk

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
	"math"
	"strconv"
	"testing"
)

const testDBDir string = "../storage/testmerk"

func TestApply(t *testing.T) {
	var batch1, batch2, batch3, batch4, batch5, batch6 Batch

	m := &Merk{}

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
	m.Apply(batch1)

	batch2 = append(batch2, op0, op1, op2, op3, op6, op8)
	m.Apply(batch2)

	batch3 = append(batch3, op4, op5, op7, op9)
	m.Apply(batch3)

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
	delKeys4, _ := m.Apply(batch4)

	require.EqualValues(t, [][]byte{[]byte("1"), []byte("5"), []byte("6"), []byte("9")}, delKeys4)
	require.NoError(t, m.tree.verify())

	batch5 = append(batch5, op12, op13, op17)
	delKeys5, _ := m.Apply(batch5)

	require.EqualValues(t, [][]byte{[]byte("2"), []byte("3"), []byte("7")}, delKeys5)
	require.NoError(t, m.tree.verify())

	batch6 = append(batch6, op10, op14, op18)
	delKeys6, _ := m.Apply(batch6)

	require.EqualValues(t, [][]byte{[]byte("0"), []byte("4"), []byte("8")}, delKeys6)
	require.Nil(t, m.tree)
}

func TestGet(t *testing.T) {
	var batch Batch

	m := &Merk{}

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
	m.Apply(batch)

	require.EqualValues(t, []byte("value0"), m.Get([]byte("key0")))
	require.EqualValues(t, []byte("value1"), m.Get([]byte("key1")))
	require.EqualValues(t, []byte("value2"), m.Get([]byte("key2")))
	require.EqualValues(t, []byte("value3"), m.Get([]byte("key3")))
	require.EqualValues(t, []byte("value4"), m.Get([]byte("key4")))
	require.EqualValues(t, []byte("value5"), m.Get([]byte("key5")))
	require.EqualValues(t, []byte("value6"), m.Get([]byte("key6")))
	require.EqualValues(t, []byte("value7"), m.Get([]byte("key7")))
	require.EqualValues(t, []byte("value8"), m.Get([]byte("key8")))
	require.EqualValues(t, []byte("value9"), m.Get([]byte("key9")))
}

func TestCommit(t *testing.T) {
	m, db := buildMerkWithDB()

	defer db.Close()
	defer db.Destroy()

	require.NoError(t, m.tree.verify())
	require.EqualValues(t, PrunedLink, m.tree.child(true).link(true).linkType())
	require.EqualValues(t, PrunedLink, m.tree.child(true).link(false).linkType())
	require.EqualValues(t, PrunedLink, m.tree.child(false).link(true).linkType())
	require.EqualValues(t, PrunedLink, m.tree.child(false).link(false).linkType())
}

func TestCommitFetchTree(t *testing.T) {
	var (
		m  *Merk
		db DB
	)

	m, db = buildMerkWithDB()

	db.Close()

	m, db, _ = New(testDBDir)
	defer db.Close()
	defer db.Destroy()

	require.NoError(t, m.tree.verify())
}

func TestCommitDel(t *testing.T) {
	var batch Batch
	m, db := buildMerkWithDB()
	defer db.Close()
	defer db.Destroy()

	op11 := &OP{O: Del, K: []byte("key1")}
	op15 := &OP{O: Del, K: []byte("key5")}
	op16 := &OP{O: Del, K: []byte("key6")}
	op19 := &OP{O: Del, K: []byte("key9")}

	batch = append(batch, op11, op15, op16, op19)
	delKeys, _ := m.Apply(batch)

	require.EqualValues(t, [][]byte{[]byte("key1"), []byte("key5"), []byte("key6"), []byte("key9")}, delKeys)
	require.NoError(t, m.tree.verify())
}

func TestTakeSnapshot(t *testing.T) {
	m, db := buildMerkWithDB()
	defer db.Close()
	defer db.Destroy()

	snapshotKey, err := TakeDBSnapshot()
	require.NoError(t, err)
	require.EqualValues(t, m.RootHash(), snapshotKey)

	var batch Batch = []*OP{
		&OP{O: Del, K: []byte("key1")},
		&OP{Put, []byte("key5"), []byte("value55")},
		&OP{O: Del, K: []byte("key8")},
		&OP{Put, []byte("key10"), []byte("value10")},
	}
	m.Apply(batch)

	err = m.Revert(snapshotKey)
	require.NoError(t, err)

	require.NoError(t, m.tree.verify())
	require.EqualValues(t, m.tree.key(), []byte("key5"))
	require.EqualValues(t, m.tree.value(), []byte("value5"))
	require.EqualValues(t, m.tree.link(true).key(), []byte("key2"))
	require.EqualValues(t, m.tree.link(true).tree().link(true).key(), []byte("key1"))
	require.EqualValues(t, m.tree.link(true).tree().link(true).tree().link(true).key(), []byte("key0"))
	require.EqualValues(t, m.tree.link(true).tree().link(false).key(), []byte("key4"))
	require.EqualValues(t, m.tree.link(true).tree().link(false).tree().link(true).key(), []byte("key3"))
	require.EqualValues(t, m.tree.link(false).key(), []byte("key8"))
	require.EqualValues(t, m.tree.link(false).tree().link(true).key(), []byte("key7"))
	require.EqualValues(t, m.tree.link(false).tree().link(true).tree().link(true).key(), []byte("key6"))
	require.EqualValues(t, m.tree.link(false).tree().link(false).key(), []byte("key9"))
}

func buildMerkWithDB() (*Merk, DB) {
	var batch Batch

	m, db, _ := New(testDBDir)

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
	m.Apply(batch)

	return m, db
}

func BenchmarkApply(b *testing.B) {
	var (
		batch Batch
		size  int = 100_000
	)

	m := &Merk{}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		batch = buildBatch(batch, size)
		b.StartTimer()

		if _, err := m.ApplyUnchecked(batch); err != nil {
			// if _, err := m.Apply(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommit(b *testing.B) {
	var (
		batch Batch
		size  int = 100_000
	)

	m, db, _ := New(testDBDir)

	defer db.Close()
	defer db.Destroy()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		batch = buildBatch(batch, size)
		b.StartTimer()

		if _, err := m.ApplyUnchecked(batch); err != nil {
			// if _, err := m.Apply(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func buildBatch(b Batch, size int) Batch {
	var batch Batch

	// create from ground
	if b == nil {
		for i := 0; i < size; i++ {
			key := blake2b.Sum256([]byte("key" + strconv.Itoa(i) + strconv.Itoa(RandIntn(math.MaxUint32))))
			val := bytes.Repeat([]byte("x"), RandIntn(1000))
			op := &OP{Put, key[:], val}
			batch = append(batch, op)
		}

		return sortBatch(batch)
	}

	// update 1/2 and delete 1/20
	for i := 0; i < size/2; i++ {
		key1 := blake2b.Sum256([]byte("key" + strconv.Itoa(i) + strconv.Itoa(RandIntn(math.MaxUint32))))
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
