package merk

type KV struct {
	key   []byte
	value []byte
	// Hash
}

func newKV(key, value []byte) *KV {
	return &KV{key, value}
}

func (kv *KV) Marshal() []byte {
	return serializeBytes(kv.key, kv.value)
}
