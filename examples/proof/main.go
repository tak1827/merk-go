package main

import (
	"fmt"
	m "github.com/tak1827/merk-go/merk"
	"github.com/tak1827/merk-go/merk/proof"
	"log"
)

const storageDir string = "../storage/example"

func main() {
	m, db, err := m.New(storageDir)
	if err != nil {
		if db != nil {
			db.Close()
		}
		log.Panic(err)
	}

	defer db.Close()   // close bager
	defer db.Destroy() // clear all data

	buildTree(m)

	// Create proof
	keys := [][]byte{[]byte("key1"), []byte("key2")}
	buf, err := proof.Prove(m.Tree, keys)
	if err != nil {
		log.Panic(err)
	}

	// Verify proof
	output, err := proof.Verify(buf, keys, m.Tree.Hash())
	if err != nil {
		log.Panic(err)
	}

	fmt.Printf("the values of keys, %v, %v\n", string(output[0]), string(output[1]))
}

func buildTree(merk *m.Merk) {

	var insertBatch m.Batch = []*m.OP{
		&m.OP{O: m.Put, K: []byte("key0"), V: []byte("value0")},
		&m.OP{O: m.Put, K: []byte("key1"), V: []byte("value1")},
		&m.OP{O: m.Put, K: []byte("key2"), V: []byte("value2")},
	}

	// apply insert
	if _, err := merk.Apply(insertBatch); err != nil {
		log.Panic(err)
	}
}
