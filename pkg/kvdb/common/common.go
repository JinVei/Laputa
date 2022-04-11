package common

import (
	"encoding/binary"
	"io"
)

const (
	MaxLevels = 12
)

type ValueType uint8

const (
	KTypeValue  = 0x01
	KTypeDelete = 0x02
)

/*
if k1 < k2,  ret < 0 (-1)
if k1 == k2, ret == 0
if k1 > k2,  0 < ret (+1)
*/

type Compare func(key1, key2 []byte) int

type FileMetaData struct {
	Refs         int
	AllowedSeeks int
	Number       uint64
	FileSize     uint64
	Smallest     InternalKey
	Largest      InternalKey
}

type InternalKey []byte

func (k InternalKey) UserKey() []byte {
	return k[:len(k)-8]
}

func (k InternalKey) Tag() uint64 {
	return binary.LittleEndian.Uint64(k[len(k)-8:])
}

func DecodeLenPrefixBytes(src []byte) ([]byte, int) {
	blen, n := binary.Varint(src)
	return src[n : n+int(blen)], (n + int(blen))
}

func EncodeLenPrefixBytes(write io.Writer, dat []byte) int {
	varintbuf := make([]byte, 10)
	blen := int64(len(dat))
	n := binary.PutVarint(varintbuf, blen)
	write.Write(varintbuf[:n])
	write.Write(dat)
	return n + int(blen)
}

func DecodeFileMetaData(src []byte) (FileMetaData, int) {
	meta := FileMetaData{}
	offset := 0

	num, n := binary.Varint(src[offset:])
	offset += n
	meta.AllowedSeeks = int(num)

	num, n = binary.Varint(src[offset:])
	offset += n
	meta.Number = uint64(num)

	num, n = binary.Varint(src[offset:])
	offset += n
	meta.FileSize = uint64(num)

	lbytes, n := DecodeLenPrefixBytes(src[offset:])
	offset += n
	meta.Smallest = InternalKey(lbytes)

	lbytes, n = DecodeLenPrefixBytes(src[offset:])
	offset += n
	meta.Largest = InternalKey(lbytes)

	return meta, offset
}

func EncodeFileMetaData(w io.Writer, meta FileMetaData) int {
	len := 0
	varintbuf := make([]byte, 10)

	n := binary.PutVarint(varintbuf, int64(meta.AllowedSeeks))
	len += n

	n = binary.PutVarint(varintbuf, int64(meta.Number))
	len += n

	n = binary.PutVarint(varintbuf, int64(meta.FileSize))
	len += n

	n = EncodeLenPrefixBytes(w, meta.Smallest)
	len += n

	n = EncodeLenPrefixBytes(w, meta.Largest)
	len += n

	return len
}

func EncodeInternalKey(w io.Writer, key []byte, sequence uint64, vtype ValueType) {
	tag := sequence<<8 | uint64(vtype)
	w.Write(key)
	tagEncode := make([]byte, 8)
	binary.LittleEndian.PutUint64(tagEncode, tag)
	w.Write(tagEncode)
}
