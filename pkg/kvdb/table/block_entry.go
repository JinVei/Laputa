package table

import "encoding/binary"

type Entry struct {
	block    BlockContent
	offset   int
	shard    int64
	unshard  int64
	valueLen int64
	deltaKey []byte
	value    []byte
}

func NewEntry() *Entry {
	entry := &Entry{}

	return entry
}

func (e *Entry) DecodeFrom(block BlockContent, offset int) int {
	e.block = block
	e.offset = offset
	n := 0
	nBytes := 0

	e.shard, n = binary.Varint(block[offset+nBytes:])
	nBytes += n

	e.unshard, n = binary.Varint(block[offset+nBytes:])
	nBytes += n

	e.valueLen, n = binary.Varint(block[offset+nBytes:])
	nBytes += n

	e.deltaKey = block[offset+nBytes : offset+nBytes+int(e.unshard)]
	nBytes += int(e.unshard)

	e.value = block[offset+nBytes : offset+nBytes+int(e.valueLen)]
	nBytes += int(e.valueLen)

	return nBytes
}

func (e *Entry) DeltaKey() []byte {
	return e.deltaKey
}

func (e *Entry) Value() []byte {
	return e.value
}

func (e *Entry) Shard() int {
	return int(e.shard)
}

func (e *Entry) Unshard() int {
	return int(e.unshard)
}

func (e *Entry) ValueLen() int {
	return int(e.valueLen)
}
