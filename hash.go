package merk

import (
	"golang.org/x/crypto/blake2b"
)

const HashSize = blake2b.Size256

type Hash [HashSize]byte

func kvHash(kv *KV) Hash {
	return blake2b.Sum256(kv.Marshal())
}

func nodeHash(kv, left, right Hash) Hash {
	return blake2b.Sum256(serializeBytes(kv[:], left[:], right[:]))
}
