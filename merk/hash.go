package merk

import (
	"golang.org/x/crypto/blake2b"
)

const HashSize = blake2b.Size256

var NullHash Hash

type Hash [HashSize]byte

func KvHash(key, value []byte) Hash {
	return blake2b.Sum256(serializeBytes(key, value))
}

func NodeHash(kv, left, right Hash) Hash {
	return blake2b.Sum256(serializeBytes(kv[:], left[:], right[:]))
}
