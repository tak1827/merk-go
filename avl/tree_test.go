package avl

import (
	"bytes"
	"testing"
	"golang.org/x/crypto/blake2b"
	"github.com/tak1827/merk-go/merk"
)

func BenchmarkInsert(b *testing.B) {
	tree := &Tree{}

	kvBuilder := func(n int) ([][]byte, [][]byte) {
		var (
			keys   [][]byte
			values [][]byte
		)
		for i := 0; i < 1000; i++ {
			key := blake2b.Sum256([]byte("key" + string(n) + string(i)))
			value := bytes.Repeat([]byte("x"), merk.RandIntn(1000))
			keys = append(keys, key[:])
			values = append(keys, value)
		}

		return keys, values
	}

	for n := 0; n < b.N; n++ {

		keys, values := kvBuilder(n)

		for i, _ := range keys {
			tree.Insert(keys[i], values[i])
		}
	}
}
