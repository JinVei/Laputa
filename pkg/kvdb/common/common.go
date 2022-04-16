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

// ---user key---
// ---   tag  ---
type InternalKey []byte

func (k InternalKey) UserKey() []byte {
	return k[:len(k)-8]
}

func (k InternalKey) Tag() uint64 {
	return binary.LittleEndian.Uint64(k[len(k)-8:])
}

func (k InternalKey) Sequence() uint64 {
	return binary.LittleEndian.Uint64(k[len(k)-8:]) >> 8
}

func (k InternalKey) KeyType() uint8 {
	return uint8(binary.LittleEndian.Uint64(k[len(k)-8:]))
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
	w.Write(varintbuf[:n])
	len += n

	n = binary.PutVarint(varintbuf, int64(meta.Number))
	w.Write(varintbuf[:n])
	len += n

	n = binary.PutVarint(varintbuf, int64(meta.FileSize))
	w.Write(varintbuf[:n])
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

func NewInternalKey(key []byte, sequence uint64, vtype ValueType) InternalKey {
	ikey := make([]byte, len(key)+8)
	tag := sequence<<8 | uint64(vtype)
	copy(ikey, key)
	binary.LittleEndian.PutUint64(ikey[len(ikey)-8:], tag)

	return ikey
}

func NewFileMetaData(number uint64, size uint64, smallest InternalKey, largest InternalKey) *FileMetaData {
	meta := new(FileMetaData)
	meta.Number = number
	meta.FileSize = size
	meta.Smallest = smallest
	meta.Largest = largest

	return meta
}

type InternalKeyCompare struct {
	userKeyCompare Compare
}

func NewInternalKeyCompare(userKeyCompare Compare) *InternalKeyCompare {
	return &InternalKeyCompare{
		userKeyCompare: userKeyCompare,
	}
}

func (cmp *InternalKeyCompare) Compare(key1, key2 []byte) int {
	ikey1 := InternalKey(key1)
	ikey2 := InternalKey(key2)
	ret := cmp.userKeyCompare(ikey1.UserKey(), ikey2.UserKey())
	if ret == 0 {
		return int(ikey2.Sequence()) - int(ikey1.Sequence())
	} else {
		return ret
	}
}
