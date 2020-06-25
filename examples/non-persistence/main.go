package main

import (
	"fmt"
	"github.com/tak1827/merk-go/merk"
	"log"
)

const storageDir string = "../storage/example"

func main() {
	m := merk.Merk{}

	var insertBatch merk.Batch = []*merk.OP{
		&merk.OP{O: merk.Put, K: []byte("key0"), V: []byte("value0")},
		&merk.OP{O: merk.Put, K: []byte("key1"), V: []byte("value1")},
		&merk.OP{O: merk.Put, K: []byte("key2"), V: []byte("value2")},
	}

	// apply insert
	if _, err := m.Apply(insertBatch, true); err != nil {
		log.Panic(err)
	}
	fmt.Printf("inserted value of key0: %v\n", string(m.Get([]byte("key0"))))

	var updateBatch merk.Batch = []*merk.OP{
		&merk.OP{O: merk.Put, K: []byte("key0"), V: []byte("value10")},
	}

	// apply update
	if _, err := m.Apply(updateBatch, true); err != nil {
		log.Panic(err)
	}
	fmt.Printf("updated value of key0: %v\n", string(m.Get([]byte("key0"))))

	var deleteBatch merk.Batch = []*merk.OP{
		&merk.OP{O: merk.Del, K: []byte("key0")},
		&merk.OP{O: merk.Del, K: []byte("key1")},
	}

	// apply delete
	deleteKeys, err := m.Apply(deleteBatch, true)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("length of deleted keys: %v\n", len(deleteKeys))
}
