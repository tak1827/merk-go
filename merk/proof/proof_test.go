package proof

import (
	"github.com/stretchr/testify/require"
	m "github.com/tak1827/merk-go/merk"
	"testing"
)

const testDBDir string = "../../storage/proofmerk"

func TestProof(t *testing.T) {
	var (
		keys   [][]byte
		buf    []byte
		output [][]byte
		err    error
	)

	tree, db := buildTree()
	defer db.Close()
	defer db.Destroy()

	keys = [][]byte{[]byte("key08")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value08")}, output)

	keys = [][]byte{[]byte("key15")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value15")}, output)

	keys = [][]byte{[]byte("key01")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value01")}, output)

	keys = [][]byte{[]byte("key01"), []byte("key15")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value01"), []byte("value15")}, output)

	keys = [][]byte{[]byte("key01"), []byte("key02"), []byte("key03"), []byte("key04")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value01"), []byte("value02"), []byte("value03"), []byte("value04")}, output)

	keys = [][]byte{[]byte("key06"), []byte("key10"), []byte("key15")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value06"), []byte("value10"), []byte("value15")}, output)

	keys = [][]byte{[]byte("key01"), []byte("key03"), []byte("key09"), []byte("key12"), []byte("key14")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value01"), []byte("value03"), []byte("value09"), []byte("value12"), []byte("value14")}, output)

	keys = [][]byte{[]byte("key02"), []byte("key03"), []byte("key05"), []byte("key07"), []byte("key09"), []byte("key10"), []byte("key12"), []byte("key13"), []byte("key15")}
	buf, err = Prove(tree, keys)
	require.NoError(t, err)
	output, err = Verify(buf, keys, tree.Hash())
	require.NoError(t, err)
	require.EqualValues(t, [][]byte{[]byte("value02"), []byte("value03"), []byte("value05"), []byte("value07"), []byte("value09"), []byte("value10"), []byte("value12"), []byte("value13"), []byte("value15")}, output)
}

func buildTree() (*m.Tree, m.DB) {
	var batch m.Batch

	merk, db, _ := m.New(testDBDir)

	op01 := &m.OP{O: m.Put, K: []byte("key01"), V: []byte("value01")}
	op02 := &m.OP{O: m.Put, K: []byte("key02"), V: []byte("value02")}
	op03 := &m.OP{O: m.Put, K: []byte("key03"), V: []byte("value03")}
	op04 := &m.OP{O: m.Put, K: []byte("key04"), V: []byte("value04")}
	op05 := &m.OP{O: m.Put, K: []byte("key05"), V: []byte("value05")}
	op06 := &m.OP{O: m.Put, K: []byte("key06"), V: []byte("value06")}
	op07 := &m.OP{O: m.Put, K: []byte("key07"), V: []byte("value07")}
	op08 := &m.OP{O: m.Put, K: []byte("key08"), V: []byte("value08")}
	op09 := &m.OP{O: m.Put, K: []byte("key09"), V: []byte("value09")}
	op10 := &m.OP{O: m.Put, K: []byte("key10"), V: []byte("value10")}
	op11 := &m.OP{O: m.Put, K: []byte("key11"), V: []byte("value11")}
	op12 := &m.OP{O: m.Put, K: []byte("key12"), V: []byte("value12")}
	op13 := &m.OP{O: m.Put, K: []byte("key13"), V: []byte("value13")}
	op14 := &m.OP{O: m.Put, K: []byte("key14"), V: []byte("value14")}
	op15 := &m.OP{O: m.Put, K: []byte("key15"), V: []byte("value15")}

	batch = append(batch, op01, op02, op03, op04, op05, op06, op07, op08, op09, op10, op11, op12, op13, op14, op15)
	merk.Apply(batch)

	return merk.Tree, db
}
