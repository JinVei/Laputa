package memtable

import (
	"encoding/binary"
)

// entry format:
//                           -------memtable key-------
//    klength  varint32
//                              ----internal key----
//    userkey  char[klength]       <- ukeyOffset
//    tag(sequence>>8|type)  uint64
//                               ----internal key---
//                           --------memtable key-------
//    vlength  varint32            <- valueOffset
//    value    char[vlength]

type Entry struct {
	// value key offset
	valueOffs uint32
	// user key offset
	ukeyOffs uint32
	tag      uint64
	buf      []byte
}

func (e Entry) UserKey() []byte {
	return e.buf[e.ukeyOffs : e.valueOffs-8]
}

func (e Entry) InternalKey() []byte {
	return e.buf[e.ukeyOffs:e.valueOffs]
}

func (e Entry) MemtableKey() []byte {
	return e.buf[:e.valueOffs]
}

func (e Entry) Value() []byte {
	if int(e.valueOffs) == len(e.buf) {
		return nil
	}
	_, n := binary.Varint(e.buf[e.valueOffs:])
	return e.buf[e.valueOffs+uint32(n):]
}

func (e Entry) ValueType() uint8 {
	return uint8(e.tag & 0xff)
}

func (e Entry) Sequence() uint64 {
	return e.tag >> 8
}

func DecodeEntry(buf []byte) Entry {
	e := Entry{}
	e.buf = buf
	len, n := binary.Varint(buf)
	idx := uint32(n)
	e.ukeyOffs = uint32(idx)
	idx = e.ukeyOffs + uint32(len) - 8
	tag := binary.BigEndian.Uint64(e.buf[idx:])
	e.tag = tag
	idx += 8
	e.valueOffs = idx
	return e
}

// func DecodeInternalKey(buf []byte) []byte {
// 	offset := 0
// 	len, n := binary.Varint(buf)
// 	offset += n
// 	return buf[offset : offset+int(len)]
// }

func VarintLen(v uint64) int {
	len := 1
	for v >= 128 {
		v >>= 7
		len++
	}
	return len
}

type LookupKey struct {
	Key      []byte
	Sequence uint64
}

func EncodeMemtableKey(uKey []byte, sequence uint64, vtype ValueType) []byte {
	tag := sequence<<8 | uint64(vtype)
	internalKeylen := uint64(len(uKey) + 8)
	varintLen := VarintLen(internalKeylen)
	buflen := varintLen + int(internalKeylen)

	buf := make([]byte, buflen)
	binary.PutVarint(buf, int64(internalKeylen))
	offset := varintLen
	copy(buf[offset:], uKey)
	offset += int(internalKeylen) - 8
	binary.BigEndian.PutUint64(buf[offset:], tag)

	return buf
}
