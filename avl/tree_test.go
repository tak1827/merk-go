package avl

import (
	"bytes"
	m "github.com/tak1827/merk-go/merk"
	"golang.org/x/crypto/blake2b"
	"math"
	"strconv"
	"testing"
)

func buildBatch(b m.Batch, size int) m.Batch {
	var batch m.Batch

	// create from ground
	if b == nil {
		for i := 0; i < size; i++ {
			key := blake2b.Sum256([]byte("key" + strconv.Itoa(i) + strconv.Itoa(m.RandIntn(math.MaxUint32))))
			val := bytes.Repeat([]byte("x"), m.RandIntn(1000))
			op := &m.OP{O: m.Put, K: key[:], V: val}
			batch = append(batch, op)
		}

		return batch
	}

	// update 1/2 and delete 1/20
	for i := 0; i < size/2; i++ {
		key1 := blake2b.Sum256([]byte("key" + strconv.Itoa(i) + strconv.Itoa(m.RandIntn(math.MaxUint32))))
		val1 := bytes.Repeat([]byte("x"), m.RandIntn(1000))
		op1 := &m.OP{O: m.Put, K: key1[:], V: val1}

		if i%20 == 0 && b[i*2].O == m.Put {
			key2 := b[i*2].K
			op2 := &m.OP{O: m.Del, K: key2}

			batch = append(batch, op1, op2)
			continue
		}

		key2 := b[i*2].K
		val2 := bytes.Repeat([]byte("x"), m.RandIntn(1000))
		op2 := &m.OP{O: m.Put, K: key2[:], V: val2}
		batch = append(batch, op2, op1)
	}

	return batch
}

func BenchmarkApply(b *testing.B) {
	var (
		batch m.Batch
		size int = 100_000
	)

	tree := &Tree{}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		batch = buildBatch(batch, size)
		b.StartTimer()

		for _, b := range batch {
			if b.O == m.Put {
				tree.Insert(b.K, b.V)
			} else {
				tree.Delete(b.K)
			}
		}
	}
}

func BenchmarkCommit(b *testing.B) {
	var (
		batch m.Batch
		size  int = 100_000
	)

	db, _ := NewBadger("../storage/testavl")
	tree := New(db)
	defer db.Close()
	defer db.db.DropAll()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		batch = buildBatch(batch, size)
		b.StartTimer()

		for _, b := range batch {
			if b.O == m.Put {
				tree.Insert(b.K, b.V)
			} else {
				tree.Delete(b.K)
			}
		}

		if err := tree.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
