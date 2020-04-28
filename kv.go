package merk

import (
  
)

type KV struct {
	key   []byte
	value []byte
	// Hash
}

func newKV(key, value []byte) *KV {
	return &KV{key, value}
}


