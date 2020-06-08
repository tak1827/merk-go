package merk

type KV struct {
	key   []byte
	value []byte
	hash  Hash
}

func newKV(key, value []byte) *KV {
	hash := KvHash(key, value)
	return &KV{key, value, hash}
}
