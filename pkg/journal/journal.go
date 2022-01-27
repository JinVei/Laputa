package journal

import (
	"errors"
	"os"
)

const (
	BlockSize            = 32 * 1024
	ChunkTypeFull   byte = 0x01
	ChunkTypeFirst  byte = 0x02
	ChunkTypeMiddle byte = 0x03
	ChunkTypeLast   byte = 0x04
	ChunkHeaderLen       = 7
)

var (
	ErrInvalidChecksum error = errors.New("Invalid checksum")
	ErrDamagedChunk    error = errors.New("Damaged Chunk")
)

/*
  Journal use for implement WAL purpose
  Journal write record to file.
  Journal file consist of amount of blocks which has fixed 32K size.
  Every block consists of chunks. And records would store in chunks.
*/
type Journal struct {
	f      *os.File
	offset uint
	blkIdx int64
	path   string
}

func New(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &Journal{
		offset: 0,
		blkIdx: 0,
		f:      f,
		path:   path,
	}, nil
}

func (l *Journal) InitAndRecover(fn func([]byte)) error {
	r, err := l.NewJournalReader()
	if err != nil {
		return err
	}
	err = r.ReplayAt(0, fn)
	if err != nil {
		return err
	}
	l.blkIdx = r.blkIdx
	l.offset = uint(r.offset)
	return nil
}

func (l *Journal) Append(data []byte) error {
	datalen := len(data)
	// chunk type is full
	if (uint(datalen) + ChunkHeaderLen) <= l.freeSpaceInBlock() {
		return l.writeChunk(NewChunk(data, ChunkTypeFull))
	}

	dataOffset := uint(0)
	dataWriter := l.newDataWriter(data, &dataOffset)

	// First Chunk
	if err := dataWriter(ChunkTypeFirst); err != nil {
		return err
	}

	for l.freeSpaceInBlock() < (uint(datalen) - dataOffset + ChunkHeaderLen) {
		// Middle Chunk
		if err := dataWriter(ChunkTypeMiddle); err != nil {
			return err
		}
	}

	// Last Chunk
	if err := dataWriter(ChunkTypeLast); err != nil {
		return err
	}

	return nil
}

func (l *Journal) newDataWriter(data []byte, offset *uint) func(chunkType byte) error {
	datalen := uint(len(data))
	return func(chunkType byte) error {
		chunkDataLen := l.freeSpaceInBlock() - ChunkHeaderLen
		if datalen < *offset+chunkDataLen {
			chunkDataLen = datalen - (*offset)
		}
		chunkData := data[*offset : *offset+chunkDataLen]
		*offset += chunkDataLen

		if err := l.writeChunk(NewChunk(chunkData, chunkType)); err != nil {
			return err
		}
		return nil
	}
}

func (l *Journal) write(b []byte) error {
	_, err := l.f.WriteAt(b, l.blkIdx*BlockSize+int64(l.offset))
	if err != nil {
		_ = l.f.Truncate(l.blkIdx*BlockSize + int64(l.offset))
		return err
	}
	l.offset += uint(len(b))
	return nil
}

func (l *Journal) writeChunk(chunk *Chunk) error {
	if err := l.alignBlock(); err != nil {
		return err
	}

	// Hasn't enough space within this block for writing data. It may cause by some bug.
	if l.freeSpaceInBlock() < uint(chunk.Len()) {
		return errors.New("Hasn't enough space within this block for writing data. It may cause by some bug.")
	}

	if err := l.write(chunk.ToBytes()); err != nil {
		return err
	}

	if err := l.alignBlock(); err != nil {
		return err
	}

	if err := l.f.Sync(); err != nil {
		return err
	}

	return nil
}

func (l *Journal) freeSpaceInBlock() uint {
	return BlockSize - l.offset
}

func (l *Journal) alignBlock() error {
	if l.freeSpaceInBlock() <= ChunkHeaderLen {
		if l.freeSpaceInBlock() == ChunkHeaderLen {
			emptyChunk := NewChunk(nil, ChunkTypeFull)
			if err := l.write(emptyChunk.ToBytes()); err != nil {
				return err
			}
		} else {
			paddingLen := l.freeSpaceInBlock()
			emptyPadding := make([]byte, paddingLen)
			if err := l.write(emptyPadding); err != nil {
				return err
			}
		}
		// Skip into next block
		l.offset = 0
		l.blkIdx++
	}
	return nil
}

func (l *Journal) NewJournalReader() (*JournalReader, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &JournalReader{
		f:     f,
		fSize: fi.Size(),
	}, nil
}

// |term:varint|index:varint|dataLen:varint|data:bytes|
