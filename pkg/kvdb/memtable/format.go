package memtable

import (
	"bytes"
	"encoding/binary"
	"io"
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
	valueOffs uint32
	ukeyOffs  uint32
	tag       uint64
	buf       []byte
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

func (e Entry) ValueType() ValueType {
	return ValueType(e.tag & 0xff)
}

func (e Entry) Sequence() uint64 {
	return e.tag >> 8
}

func (e Entry) Bytes() []byte {
	return e.buf
}

func DecodeEntry(buf []byte) Entry {
	e := Entry{}
	e.buf = buf
	len, n := binary.Varint(buf)
	idx := uint32(n)
	e.ukeyOffs = uint32(idx)
	idx = e.ukeyOffs + uint32(len) - 8
	tag := binary.LittleEndian.Uint64(e.buf[idx:])
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

func NewMemtableKey(uKey []byte, sequence uint64, vtype ValueType) []byte {
	tag := sequence<<8 | uint64(vtype)
	internalKeylen := uint64(len(uKey) + 8)
	varintLen := VarintLen(internalKeylen)
	buflen := varintLen + int(internalKeylen)

	buf := make([]byte, buflen)
	binary.PutVarint(buf, int64(internalKeylen))
	offset := varintLen
	copy(buf[offset:], uKey)
	offset += int(internalKeylen) - 8
	binary.LittleEndian.PutUint64(buf[offset:], tag)

	return buf
}

func EncodeMemtableKey(w io.Writer, uKey []byte, tag uint64) {
	internalKeylen := uint64(len(uKey) + 8)
	varintLen := VarintLen(internalKeylen)

	varintbuf := make([]byte, varintLen)
	n := binary.PutVarint(varintbuf, int64(internalKeylen))
	w.Write(varintbuf[:n])

	var tagbuf [8]byte
	w.Write(uKey)
	binary.LittleEndian.PutUint64(tagbuf[:], tag)
	w.Write(tagbuf[:])
}

func NewEntry(sequence uint64, key, value []byte, vtype ValueType) *Entry {
	tag := sequence<<8 | uint64(vtype)
	internalKeylen := uint64(len(key) + 8)
	keyVarintLen := VarintLen(internalKeylen)

	valueLen := len(value)
	valueVarintLen := VarintLen(uint64(valueLen))

	buflen := keyVarintLen + int(internalKeylen) + valueVarintLen + valueLen
	buf := make([]byte, buflen)
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()

	EncodeMemtableKey(buffer, key, tag)

	varintbuf := make([]byte, valueVarintLen)
	n := binary.PutVarint(varintbuf, int64(valueLen))
	buffer.Write(varintbuf[:n])

	buffer.Write(value)

	entry := new(Entry)
	entry.buf = buf
	entry.tag = tag
	return entry
}
