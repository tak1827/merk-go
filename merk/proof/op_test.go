package proof

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	op1 := &OP{t: Push, n: &Node{t: Hash, h: blake2b.Sum256([]byte("pHash"))}}
	op2 := &OP{t: Push, n: &Node{t: KVHash, h: blake2b.Sum256([]byte("kvHash"))}}
	op3 := &OP{t: Push, n: &Node{t: KV, k: []byte("key"), v: []byte("value")}}
	op4 := &OP{t: Parent}
	op5 := &OP{t: Child}

	var ops []*OP = []*OP{op1, op2, op3, op4, op5}

	buf := encode(ops)

	var op *OP
	op, buf = decode(buf)
	require.EqualValues(t, op1, op)
	op, buf = decode(buf)
	require.EqualValues(t, op2, op)
	op, buf = decode(buf)
	require.EqualValues(t, op3, op)
	op, buf = decode(buf)
	require.EqualValues(t, op4, op)
	op, buf = decode(buf)
	require.EqualValues(t, op5, op)
}
