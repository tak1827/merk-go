package proof

import (
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	m "github.com/tak1827/merk-go/merk"
)

var treeMap map[string]*Tree

type Tree struct {
	node  *Node
	left  *Tree
	right *Tree
}

func newTree(n *Node) (t *Tree) {
	t = &Tree{node: n}
	t.intoMap()
	return
}

func (t *Tree) child(isLeft bool) *Tree {
	if isLeft {
		return t.left
	}
	return t.right
}

func (t *Tree) intoMap() {
	if t.node.t == Hash || t.node.t == KVHash {
		treeMap[string(t.node.h[:])] = t
	} else {
		treeMap[string(t.node.k[:])] = t
	}
}

func (t *Tree) setChild(isLeft bool, child *Tree) {
	if isLeft {
		t.left = child
	} else {
		t.right = child
	}
}

func (t *Tree) attach(isLeft bool, child *Tree) {
	if t.child(isLeft) != nil {
		panic("BUG: tried to attach to child, but it is already occupied")
	}

	t.setChild(isLeft, child.intoHash())
}

func (t *Tree) childHash(isLeft bool) m.Hash {
	var child *Tree = t.child(isLeft)

	if child == nil {
		return m.NullHash
	}
	return child.hash()
}

func (t *Tree) intoHash() (hashTree *Tree) {
	hashNode := func(tree *Tree, kvHash m.Hash) *Node {
		h := m.NodeHash(
			kvHash,
			t.childHash(true),
			t.childHash(false),
		)
		return &Node{t: Hash, h: h}
	}

	switch t.node.t {
	case Hash:
		hashTree = &Tree{node: t.node}
		hashTree.intoMap()
		return
	case KVHash:
		hashTree = &Tree{node: hashNode(t, t.node.h)}
		hashTree.intoMap()
		return
	case KV:
		kvh := m.KvHash(t.node.k, t.node.v)
		hashTree = &Tree{node: hashNode(t, kvh)}
		hashTree.intoMap()
		return
	default:
		panic("BUG: undefined tree note type")
	}
}

func (t *Tree) hash() (h m.Hash) {
	if t.node.t != Hash {
		panic("BUG: expected Node Hsh")
	}
	return t.node.h
}

func verify(buf []byte, keys [][]byte, expectedHash m.Hash) ([][]byte, error) {
	var (
		stack  []*Tree
		output [][]byte
		op     *OP
		key    []byte
		// parent, child *Tree
	)

	treeMap = make(map[string]*Tree)

	var keyIndex = 0
	var lastPush *Node

	tryPop := func(s []*Tree) (*Tree, []*Tree) {
		if len(s) == 0 {
			panic("BUG: stack underflow")
		}
		target := s[len(s)-1]
		// rest := s[:len(s)-1]
		rest := make([]*Tree, len(s)-1)
		copy(rest, s[:len(s)-1])
		return target, rest
	}

	var kim int = 1

	for {
		if len(buf) <= 0 {
			break
		}

		// spew.Dump("++++++++++++++++++++++++++++++++++++++++")
		// spew.Dump(len(stack))

		op, buf = decode(buf)
		switch op.t {
		case Parent:
			// var parent, child *Tree
			fmt.Println("Parent")
			parent := stack[len(stack)-1]
			child := stack[len(stack)-2]
			parent.attach(true, child)
			_, stack = tryPop(stack)
			_, stack = tryPop(stack)
			// stack := stack[:len(stack)-2]
			stack = append(stack, parent)

			// spew.Dump("---------------------- 1")
			// spew.Dump(len(stack))
			// spew.Dump(stack)

		case Child:
			// var parent, child *Tree
			fmt.Println("Child")
			child := stack[len(stack)-1]
			parent := stack[len(stack)-2]
			parent.attach(false, child)
			_, stack = tryPop(stack)
			_, stack = tryPop(stack)
			// stack := stack[:len(stack)-2]
			stack = append(stack, parent)

			spew.Dump("---------------------- 2")

			// if kim == 2 {
			// 	spew.Dump(")))))")
			// 	// spew.Dump(parent)
			// 	spew.Dump(stack)
			// 	spew.Dump(stack[0].intoHash())
			// 	panic("child")
			// }

			kim++

		case Push:
			if op.n.t == Hash {
				fmt.Printf("Push::Hash %v\n", op.n.h)
			}

			if op.n.t == KVHash {
				fmt.Printf("Push::KVHash %v\n", op.n.h)
			}

			if op.n.t == KV {
				fmt.Printf("Push::KV %v\n", string(op.n.k))
			}

			// spew.Dump("---------------------- 3")
			// spew.Dump(stack)

			pushTree := &Tree{node: op.n}
			pushTree.intoMap()
			stack = append(stack, pushTree)

			// spew.Dump("---------------------- 4")
			// spew.Dump(stack)

			if op.n.t == KV {
				key = op.n.k

				if lastPush != nil && lastPush.t == KV && string(key) <= string(lastPush.k) {
					panic("BUG: incorrect key ordering")
				}

				for {
					if keyIndex >= len(keys) || string(key) < string(keys[keyIndex]) {
						break
					} else if string(key) == string(keys[keyIndex]) {
						// KV for queried key
						output = append(output, op.n.v)
						// 			spew.Dump("----------------------")
						// spew.Dump(stack)
						// panic("fuga")
					} else if string(key) > string(keys[keyIndex]) {
						if lastPush == nil {
							// previous push was a boundary (global edge or lower key),
							// so this is a valid absence proof
							output = append(output, []byte{})
						} else {
							// proof is incorrect since it skipped queried keys
							panic("BUG: proof incorrectly formed")
						}
					}

					keyIndex++
				}
			}

			lastPush = op.n

		default:
			panic("BUG: undefined proof OP type")
		}
	}

	spew.Dump("*********************")
	spew.Dump(len(stack))
	spew.Dump(stack)

	// panic("path here")

	// absence proofs for right edge
	if keyIndex < len(keys) {
		if lastPush.t != KV {
			return nil, errors.New("proof incorrectly formed")
		}
		for i := keyIndex; i < len(keys); i++ {
			output = append(output, []byte{})
		}
	} else {
		if len(keys) != len(output) {
			return nil, errors.New("output length is not same as keys length")
		}
	}

	if len(stack) != 1 {
		return nil, errors.New("expected proof to result in exactly one stack item")
	}

	// root, _ := tryPop(stack)
	root := stack[len(stack)-1]
	hash := root.intoHash().hash()

	spew.Dump(output)

	if hash != expectedHash {
		return nil, fmt.Errorf("proof did not match expected hash, expected: %v, actual: %v", expectedHash, hash)
	}

	return output, nil
}
