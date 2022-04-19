package log

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type Reader struct {
	blkBuf      *bytes.Buffer // block buffer
	recordBuf   *bytes.Buffer
	eof         bool
	file        *os.File
	lasterr     error
	offset      int
	corrupted   bool
	blkbufBytes []byte
}

func NewReader(file *os.File) *Reader {
	r := &Reader{}
	r.blkBuf = bytes.NewBuffer(nil)
	r.recordBuf = bytes.NewBuffer(make([]byte, 4*1024)) // 4K
	//r.blkBuf.Reset()
	r.file = file
	r.blkbufBytes = make([]byte, BlockSize)
	return r
}

// BE CAUTIOUS: the result is a slice of buffer, and would be override in next ReadRecord
func (r *Reader) ReadRecord() ([]byte, error) {
	r.recordBuf.Reset()
	for {
		record, rtype, err := r.readPhysicalRecord()
		if err != nil {
			return nil, err
		}

		switch rtype {
		case RecordTypeFull:
			return record, nil
		case RecordTypeFirst:
			r.recordBuf.Write(record)
		case RecordTypeMiddle:
			r.recordBuf.Write(record)
		case RecordTypeLast:
			r.recordBuf.Write(record)
			return r.recordBuf.Next(r.recordBuf.Len()), nil
		}
	}
}

func (r *Reader) readPhysicalRecord() ([]byte, RecordType, error) {
	if r.blkBuf.Len() <= HeaderSize {
		if !r.eof {
			// the remain bytes less than HeaderSize. skip the tail of block
			n, err := r.file.Read(r.blkbufBytes)
			if err != nil {
				r.eof = true
				r.lasterr = err
				return nil, 0, err
			}
			if n < BlockSize {
				r.eof = true
			}
			*r.blkBuf = *bytes.NewBuffer(r.blkbufBytes[:n])
		} else {
			return nil, 0, io.EOF
		}
	}

	header := r.blkBuf.Next(HeaderSize)
	headercrc := binary.LittleEndian.Uint32(header)
	length := binary.LittleEndian.Uint16(header[4:6])
	rtype := header[6]
	available := r.blkBuf.Len()

	if available < int(length) {
		//  Corruption
		r.corrupted = true
		return nil, 0, ErrCorruption
	}
	record := r.blkBuf.Next(int(length))
	crc := crc32.ChecksumIEEE(header[4:])
	crc = crc32.Update(crc, crc32.IEEETable, record)
	if crc != headercrc {
		r.corrupted = true
		return nil, 0, ErrCorruption
	}
	r.offset += HeaderSize + int(length)
	return record, RecordType(rtype), nil
}
