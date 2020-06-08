package proof

import (
	"fmt"
	"github.com/lithdew/bytesutil"
	m "github.com/tak1827/merk-go/merk"
)

type OPType uint8

const (
	Push OPType = 1 << iota
	Parent
	Child
)

type OP struct {
	t OPType
	n *Node
}

func newKVNode(tree *m.Tree) *OP {
	return &OP{t: Push, n: &Node{t: KV, k: tree.Key(), v: tree.Value()}}
}

func newKVHashNode(tree *m.Tree) *OP {
	return &OP{t: Push, n: &Node{t: KVHash, h: tree.KvHash()}}
}

func newHashNode(link m.Link) *OP {
	return &OP{t: Push, n: &Node{t: Hash, h: link.Hash()}}
}

func (o *OP) encodeOP(output []byte) []byte {
	var kLen, vLen uint32

	switch o.t {
	case Push:
		if o.n.t == Hash {
			output = append(output, byte(0x01))
			return append(output, o.n.h[:]...)
		}

		if o.n.t == KVHash {
			output = append(output, byte(0x02))
			return append(output, o.n.h[:]...)
		}

		output = append(output, byte(0x03))
		kLen = uint32(len(o.n.k))
		output = append(output, bytesutil.AppendUint32BE(nil, kLen)...)
		output = append(output, o.n.k...)
		vLen = uint32(len(o.n.v))
		output = append(output, bytesutil.AppendUint32BE(nil, vLen)...)
		return append(output, o.n.v...)
	case Parent:
		return append(output, byte(0x10))

	case Child:
		return append(output, byte(0x11))

	default:
		panic("BUG: undefined proof op type")
	}
}

// func (o *OP) encodeLen() int {
// 	switch o.t {
// 	case Push:
// 		if o.n.t == Hash {
// 			return 1 + m.HashSize
// 		}

// 		if o.n.t == KVHash {
// 			return 1 + m.HashSize
// 		}

// 		return 9 + len(o.n.k) + len(o.n.v)

// 	case Parent:
// 		return 1

// 	case Child:
// 		return 1
// 	default:
// 		panic("BUG: undefined proof op type")
// 	}
// }

func encode(ops []*OP) (buf []byte) {
	for _, op := range ops {
		buf = append(buf, op.encodeOP(nil)...)
	}
	return
}

func decode(buf []byte) (*OP, []byte) {
	var (
		t                  byte
		h                  m.Hash
		kLen, vLen         uint32
		hBytes, key, value []byte
	)

	t, buf = buf[0], buf[1:]

	switch t {
	case byte(0x01):
		hBytes, buf = buf[:m.HashSize], buf[m.HashSize:]
		copy(h[:], hBytes)
		return &OP{t: Push, n: &Node{t: Hash, h: h}}, buf

	case byte(0x02):
		hBytes, buf = buf[:m.HashSize], buf[m.HashSize:]
		copy(h[:], hBytes)
		return &OP{t: Push, n: &Node{t: KVHash, h: h}}, buf

	case byte(0x03):
		kLen, buf = bytesutil.Uint32BE(buf[:4]), buf[4:]
		key, buf = buf[:kLen], buf[kLen:]
		vLen, buf = bytesutil.Uint32BE(buf[:4]), buf[4:]
		value, buf = buf[:vLen], buf[vLen:]
		return &OP{t: Push, n: &Node{t: KV, k: key, v: value}}, buf

	case byte(0x10):
		return &OP{t: Parent}, buf

	case byte(0x11):
		return &OP{t: Child}, buf

	default:
		panic("BUG: undefined proof op type")
	}
}

func displayOps(ops []*OP) {
	for _, op := range ops {
		switch op.t {
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

		case Parent:
			fmt.Println("Parent")

		case Child:
			fmt.Println("Child")

		default:
			panic("BUG: undefined proof op type")
		}
	}
}
