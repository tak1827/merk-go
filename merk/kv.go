package merk

type KV struct {
	key   []byte
	value []byte
	hash  Hash
}

func newKV(key, value []byte) *KV {
	hash := kvHash(key, value)
	return &KV{key, value, hash}
}
