package journal

import (
	"crypto/md5"
	"encoding/binary"
)

// |checksum:4|chunk_type:1|len:2|data|
type Chunk struct {
	ChunkHeader
	Data []byte
}

// ChunkHeader occupy first 7 byte of chunk
type ChunkHeader struct {
	checksum [4]byte
	ctype    [1]byte
	len      [2]byte
}

func NewChunkHeader(b []byte) *ChunkHeader {
	ch := &ChunkHeader{}
	copy(ch.checksum[:], b[0:4])
	copy(ch.ctype[:], b[4:5])
	copy(ch.len[:], b[5:7])
	return ch
}

func NewChunk(data []byte, chunkType byte) *Chunk {
	c := Chunk{}
	datalen := uint(len(data))

	binary.LittleEndian.PutUint16(c.len[:], uint16(datalen))
	c.ctype[0] = chunkType
	c.Data = data

	buf := []byte{}
	buf = append(buf, c.ctype[:]...)
	buf = append(buf, c.len[:]...)
	buf = append(buf, c.Data...)

	checksum := md5.Sum(buf)
	copy(c.checksum[:], checksum[0:4])

	return &c
}

func (c *Chunk) ToBytes() []byte {
	buf := append([]byte{}, c.checksum[:]...)
	buf = append(buf, c.ctype[:]...)
	buf = append(buf, c.len[:]...)
	buf = append(buf, c.Data...)
	return buf
}

func (c *ChunkHeader) DataLen() uint16 {
	return binary.LittleEndian.Uint16(c.len[:])
}

func (c *ChunkHeader) Len() uint16 {
	return c.DataLen() + ChunkHeaderLen
}

func (c *ChunkHeader) Type() byte {
	return c.ctype[0]
}

func (c *ChunkHeader) Checksum() [4]byte {
	return c.checksum
}
