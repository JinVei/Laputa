package table

import (
	"Laputa/pkg/util"
	"bytes"
	"encoding/binary"
)

// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

type BlockBuilder struct {
	buffer      *bytes.Buffer
	restarts    []uint32
	numRestarts int
	lastKey     *bytes.Buffer
	counter     int // Number of entries emitted since restart
	finished    bool

	restartInterval int // todo
}

func NewBlockBuilder(restartInterval int) *BlockBuilder {
	builder := &BlockBuilder{}
	builder.buffer = bytes.NewBuffer(make([]byte, 0, 1*1024))
	builder.restarts = make([]uint32, 0, 10)
	builder.restartInterval = restartInterval
	builder.lastKey = bytes.NewBuffer(make([]byte, 0, 10))
	return builder
}

func (builder *BlockBuilder) Reset() {
	builder.buffer.Reset()
	builder.lastKey.Reset()
	builder.restarts = builder.restarts[:0]
	builder.numRestarts = 0
	builder.finished = false
	builder.counter = 0
}

func (builder *BlockBuilder) Add(key, value []byte) {
	util.Assert(!builder.finished)

	var sharedLen int
	if 0 == builder.counter%builder.restartInterval {
		builder.restarts = append(builder.restarts, uint32(builder.buffer.Len()))
		builder.numRestarts++
		builder.lastKey.Reset()
	}

	sharedLen = commonPrefix(builder.lastKey.Bytes(), key)
	varint32 := make([]byte, 10, 10)
	unsharedLen := len(key) - sharedLen
	valueLen := len(value)
	deltaKey := key[sharedLen:]

	n := binary.PutVarint(varint32, int64(sharedLen))
	builder.buffer.Write(varint32[:n])

	n = binary.PutVarint(varint32, int64(unsharedLen))
	builder.buffer.Write(varint32[:n])

	n = binary.PutVarint(varint32, int64(valueLen))
	builder.buffer.Write(varint32[:n])

	builder.buffer.Write(deltaKey)
	builder.buffer.Write(value)

	builder.lastKey.Reset()
	builder.lastKey.Write(key)
	builder.counter++

}

// BE CAUTIOUS: The result is ref to the buffer of builder
// And reuse builder would may override result
func (builder *BlockBuilder) Finish() BlockContent {
	// fill restarts and number of restarts
	fixed32Bytes := make([]byte, 4)
	for i := 0; i < builder.numRestarts; i++ {
		binary.LittleEndian.PutUint32(fixed32Bytes, builder.restarts[i])
		builder.buffer.Write(fixed32Bytes)
	}

	binary.LittleEndian.PutUint32(fixed32Bytes, uint32(builder.numRestarts))
	builder.buffer.Write(fixed32Bytes)

	builder.finished = true
	return builder.buffer.Bytes()
}

func (builder *BlockBuilder) EstimatedSize() int {
	return builder.buffer.Len()
}

func commonPrefix(key1, key2 []byte) int {
	i := 0
	for i < len(key1) && i < len(key2) && key1[i] == key2[i] {
		i++
	}
	return i
}

func (builder *BlockBuilder) Empty() bool {
	return builder.buffer.Len() <= 0
}
