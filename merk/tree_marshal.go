package merk

import (
	"fmt"
	"github.com/valyala/bytebufferpool"
	"math"
)

const (
	noLinkExist uint8 = iota
	leftLinkExist
	rightLinkExist
	bothLinkExist
)

func (t *Tree) linkFlg() uint8 {
	var left Link = t.link(true)
	var right Link = t.link(false)

	if left == nil && right == nil {
		return noLinkExist
	} else if left != nil && right == nil {
		return leftLinkExist
	} else if left == nil && right != nil {
		return rightLinkExist
	}

	return bothLinkExist
}

func (t *Tree) marshal(pool *bytebufferpool.Pool) []byte {

	b := pool.Get()
	defer pool.Put(b)

	var buf []byte = b.B

	// write kv hash
	hash := t.kvHash()
	buf = append(buf, hash[:]...)

	// write links
	var flg uint8 = t.linkFlg()
	buf = append(buf, byte(flg))

	if t.link(true) != nil {
		buf = marshalLink(t.link(true), (buf))
	}

	if t.link(false) != nil {
		buf = marshalLink(t.link(false), (buf))
	}

	// write kv value
	if uint32(len(t.value())) > uint32(math.MaxUint32) {
		panic(fmt.Sprintf("BUG: too long, t.value(): %v ", t.value()))
	}
	buf = append(buf, t.value()...)

	return append(make([]byte, 0), buf...)
}

func unmarshalTree(key, buf []byte) *Tree {
	var (
		leftLink, rightLink Link
		hashSlice           []byte
		flg                 uint8
	)

	kv := new(KV)

	kv.key = key

	// read hash
	hashSlice, buf = buf[:HashSize], buf[HashSize:]
	copy(kv.hash[:], hashSlice)

	// read links
	flg, buf = uint8(buf[:1][0]), buf[1:]

	if flg == leftLinkExist || flg == bothLinkExist {
		leftLink, buf = unmarshalPruned(buf)
	}

	if flg == rightLinkExist || flg == bothLinkExist {
		rightLink, buf = unmarshalPruned(buf)
	}

	// read value
	kv.value = buf

	return &Tree{kv, leftLink, rightLink}
}
