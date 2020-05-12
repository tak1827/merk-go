package merk

import (
	"fmt"
	"github.com/lithdew/bytesutil"
	"math"
)

func marshalLink(l Link, buf []byte) []byte {
	// var buf64 [8]byte
	var key []byte = l.key()

	// write hash
	hash := l.hash()
	buf = append(buf, hash[:]...)

	// write child heights
	buf = append(buf, byte(l.childHeights()[0]))
	buf = append(buf, byte(l.childHeights()[1]))

	// write key
	if uint32(len(key)) > uint32(math.MaxUint32) {
		panic(fmt.Sprintf("BUG: too long, key: %v ", key))
	}
	buf = bytesutil.AppendUint32BE(buf, uint32(len(key)))
	buf = append(buf, key...)
	return buf
}

func unmarshalPruned(buf []byte) (*Pruned, []byte) {
	var (
		hashSlice []byte
		len       uint32
	)

	p := new(Pruned)

	// read hash
	hashSlice, buf = buf[:HashSize], buf[HashSize:]
	copy(p.h[:], hashSlice)

	// read left child height
	p.ch[0], buf = uint8(buf[:1][0]), buf[1:]
	p.ch[1], buf = uint8(buf[:1][0]), buf[1:]

	// read key
	len, buf = bytesutil.Uint32BE(buf[:4]), buf[4:]
	p.k, buf = buf[:len], buf[len:]

	return p, buf
}
