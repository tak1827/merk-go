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

func kvFromFields(key, value []byte, hash Hash) *KV {
	return &KV{key, value, hash}
}

func (kv *KV) kvWithValue(value []byte) {
	kv.value = value
	kv.hash = kvHash(kv.key, kv.value)
}
