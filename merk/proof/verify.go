package proof

import (
	"errors"
	"fmt"
	m "github.com/tak1827/merk-go/merk"
)

type Tree struct {
	node  *Node
	left  *Tree
	right *Tree
}

func newTree(n *Node) (t *Tree) {
	return &Tree{node: n}
}

func (t *Tree) child(isLeft bool) *Tree {
	if isLeft {
		return t.left
	}
	return t.right
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

func (t *Tree) intoHash() *Tree {
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
		return &Tree{node: t.node}
	case KVHash:
		return &Tree{node: hashNode(t, t.node.h)}
	case KV:
		kvh := m.KvHash(t.node.k, t.node.v)
		return &Tree{node: hashNode(t, kvh)}
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

func Verify(buf []byte, keys [][]byte, expectedHash m.Hash) ([][]byte, error) {
	var (
		op            *OP
		stack         []*Tree
		output        [][]byte
		parent, child *Tree
		key           []byte
		keyIndex      int
		lastPush      *Node
	)

	pop := func(s []*Tree) (*Tree, []*Tree) {
		if len(s) == 0 {
			panic("BUG: stack underflow")
		}
		target := s[len(s)-1]

		rest := make([]*Tree, len(s)-1)
		copy(rest, s[:len(s)-1])

		return target, rest
	}

	for {
		if len(buf) <= 0 {
			break
		}

		op, buf = decode(buf)
		switch op.t {
		case Parent:
			parent, stack = pop(stack)
			child, stack = pop(stack)
			parent.attach(true, child)
			stack = append(stack, parent)

		case Child:
			child, stack = pop(stack)
			parent, stack = pop(stack)
			parent.attach(false, child)
			stack = append(stack, parent)

		case Push:
			stack = append(stack, &Tree{node: op.n})

			if op.n.t == KV {
				key = op.n.k

				if lastPush != nil && lastPush.t == KV && string(key) <= string(lastPush.k) {
					return nil, fmt.Errorf("incorrect key ordering key: %v", string(key))
				}

				for {
					if keyIndex >= len(keys) || string(key) < string(keys[keyIndex]) {
						break
					} else if string(key) == string(keys[keyIndex]) {
						// KV for queried key
						output = append(output, op.n.v)
					} else if string(key) > string(keys[keyIndex]) {
						if lastPush == nil {
							// previous push was a boundary (global edge or lower key),
							// so this is a valid absence proof
							output = append(output, []byte{})
						} else {
							// proof is incorrect since it skipped queried keys
							return nil, fmt.Errorf("proof incorrectly formed key: %v", key)
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

	root := stack[len(stack)-1]
	hash := root.intoHash().hash()

	if hash != expectedHash {
		return nil, fmt.Errorf("proof did not match expected hash, expected: %v, actual: %v", expectedHash, hash)
	}

	return output, nil
}
