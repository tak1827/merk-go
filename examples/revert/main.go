package main

import (
	"bytes"
	"fmt"
	m "github.com/tak1827/merk-go/merk"
	"log"
)

const storageDir string = "../storage/example"

func main() {
	merk, db, err := m.New(storageDir)
	if err != nil {
		if db != nil {
			db.Close()
		}
		log.Panic(err)
	}

	defer db.Close()   // close bager
	defer db.Destroy() // clear all data

	buildTree(merk)

	// take snapshot
	snapshotKey, err := m.TakeDBSnapshot()
	if err != nil {
		log.Panic(err)
	}
	root := merk.RootHash()
	fmt.Printf("root hash is equal to snapshot key, %v\n", bytes.Compare(root[:], snapshotKey[:]))

	var batch m.Batch = []*m.OP{
		&m.OP{O: m.Del, K: []byte("key0")},
		&m.OP{O: m.Del, K: []byte("key1")},
		&m.OP{O: m.Put, K: []byte("key3"), V: []byte("value3")},
	}
	merk.Apply(batch)

	// revert tree using snapshot key
	if err = merk.Revert(snapshotKey); err != nil {
		log.Panic(err)
	}
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
