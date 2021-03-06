package journal

import (
	"crypto/md5"
	"os"
)

// JournalReader is a reader for journal
type JournalReader struct {
	f      *os.File
	offset int64
	blkIdx int64
	fSize  int64
}

func (r *JournalReader) ReplayAt(blkIdx int64, fn func([]byte)) error {
	if r.fSize == 0 {
		return nil
	}
	if err := r.ResetOffset(blkIdx, 0); err != nil {
		return err
	}

	if err := r.skipIncompleteChunk(); err != nil {
		return err
	}
	for !r.IsEnd() {
		d, err := r.ReadOneRecord()
		if err != nil {
			return err
		}
		if fn != nil {
			fn(d)
		}
	}

	return nil
}

func (r *JournalReader) ResetOffset(blkIdx, offset int64) error {
	if _, err := r.f.Seek(blkIdx*BlockSize+offset, 0); err != nil {
		return err
	}
	r.blkIdx = blkIdx
	r.offset = offset
	return nil
}

func (r *JournalReader) skipIncompleteChunk() error {
	for {
		curtBlkIdn := r.blkIdx
		curtOffset := r.offset
		chunk, err := r.readOneChunk()
		if err != nil {
			return err
		}
		if chunk.Type() == ChunkTypeFirst || chunk.Type() == ChunkTypeFull {
			if err := r.ResetOffset(curtBlkIdn, curtOffset); err != nil {
				return err
			}
			return nil
		}
	}
}

func (r *JournalReader) alignBlock() error {
	freeSpaceLen := BlockSize - r.offset
	if freeSpaceLen <= ChunkHeaderLen {
		r.blkIdx++
		r.offset = 0
		if _, err := r.f.Seek(r.blkIdx*BlockSize+r.offset, 0); err != nil {
			return err
		}
	}
	return nil
}

func (r *JournalReader) ReadOneRecord() ([]byte, error) {
	buf := []byte{}
	for {
		chunk, err := r.readOneChunk()
		if err != nil {
			return nil, err
		}
		buf = append(buf, chunk.Data...)
		if chunk.Type() == ChunkTypeFull || chunk.Type() == ChunkTypeLast {
			return buf, nil
		}
	}
}

func (r *JournalReader) readOneChunk() (*Chunk, error) {
	headerB := make([]byte, ChunkHeaderLen)
	n, err := r.f.Read(headerB)
	if err != nil {
		return nil, err
	}
	if n != ChunkHeaderLen {
		return nil, ErrDamagedChunk
	}
	ch := NewChunkHeader(headerB)
	data := make([]byte, ch.DataLen())
	n, err = r.f.Read(data)
	if err != nil {
		return nil, err
	}
	if n != int(ch.DataLen()) {
		return nil, ErrDamagedChunk
	}
	checkData := append(ch.ctype[:], ch.len[:]...)
	checkData = append(checkData, data...)

	if !verifyChecksum(checkData, ch.checksum[:]) {
		return nil, ErrInvalidChecksum
	}

	r.offset += int64(ch.Len())

	if err := r.alignBlock(); err != nil {
		return nil, err
	}

	c := &Chunk{
		ChunkHeader: *ch,
		Data:        data,
	}

	return c, nil
}

func (r *JournalReader) IsEnd() bool {
	return r.fSize <= r.blkIdx*BlockSize+r.offset
}

func verifyChecksum(data []byte, checksum []byte) bool {
	dataChecksum := md5.Sum(data)
	for i := 0; i < 4; i++ {
		if dataChecksum[i] != checksum[i] {
			return false
		}
	}
	return true
}

func (r *JournalReader) GetOffset() int64 {
	return r.blkIdx*BlockSize + r.offset
}
