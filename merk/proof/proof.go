package proof

import (
	m "github.com/tak1827/merk-go/merk"
)

func createProof(tree *m.Tree, keys [][]byte) ([]*OP, []bool) {
	var leftKeys, rightKeys [][]byte

	found, mid := m.BinarySearch(tree.Key(), keys)

	if found {
		leftKeys, rightKeys = keys[:mid], keys[mid+1:]
	} else {
		leftKeys, rightKeys = keys[:mid], keys[mid:]
	}

	proof, leftAbsence := createChildProof(tree, true, leftKeys)
	proofRight, rightAbsence := createChildProof(tree, false, rightKeys)

	hasLeft, hasRight := len(proof) != 0, len(proofRight) != 0

	if found || leftAbsence[1] || rightAbsence[0] {
		proof = append(proof, newKVNode(tree))
	} else {
		proof = append(proof, newKVHashNode(tree))
	}

	if hasLeft {
		proof = append(proof, &OP{t: Parent})
	}

	if hasRight {
		proof = append(proof, proofRight...)
		proof = append(proof, &OP{t: Child})
	}

	return proof, []bool{leftAbsence[0], rightAbsence[1]}
}

func createChildProof(tree *m.Tree, isLeft bool, keys [][]byte) ([]*OP, []bool) {
	var l m.Link = tree.Link(isLeft)

	if len(keys) == 0 && l == nil {
		return nil, []bool{false, false}
	}

	if len(keys) == 0 {
		return []*OP{newHashNode(l)}, []bool{false, false}
	}

	var child *m.Tree = tree.Child(isLeft)

	if child == nil {
		return nil, []bool{true, true}
	}

	return createProof(child, keys)
}
