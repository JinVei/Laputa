package log

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"Laputa/pkg/util"
)

type Writer struct {
	blockOffset int // Current offset in block
	dest        *os.File
}

func NewWriter(file *os.File) *Writer {
	w := &Writer{}
	w.dest = file
	return w
}

func (w *Writer) AddRecord(record []byte) (err error) {
	left := len(record)
	begin := true
	end := false
	offset := 0

	for !end && err == nil {
		leftover := BlockSize - w.blockOffset
		util.Assert(0 <= leftover)

		// There is no enough space in block to store a record
		// So fill some trashs and skip to next block
		if leftover < HeaderSize {
			if 0 < leftover {
				empyty7 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
				w.dest.Write(empyty7[:leftover])
			}
			w.blockOffset = 0
		}
		util.Assert(0 <= BlockSize-w.blockOffset-HeaderSize)

		available := BlockSize - w.blockOffset - HeaderSize

		fragLen := available
		if left <= available {
			end = true
			fragLen = left
		}

		recordType := RecordTypeMiddle
		if begin && end {
			recordType = RecordTypeFull
		} else if begin {
			recordType = RecordTypeFirst
		} else if end {
			recordType = RecordTypeLast
		}

		err = w.EmitPhysicalRecord(recordType, record[offset:offset+fragLen])

		offset += fragLen
		w.blockOffset += fragLen + HeaderSize
		begin = false
		left -= fragLen
	}

	return err
}

func (w *Writer) EmitPhysicalRecord(rtype RecordType, record []byte) error {
	header := make([]byte, 7)

	rlen := uint16(len(record))
	binary.LittleEndian.PutUint16(header[4:6], rlen)
	header[6] = byte(rtype)
	crc := crc32.ChecksumIEEE(header[4:])
	crc = crc32.Update(crc, crc32.IEEETable, record)
	binary.LittleEndian.PutUint32(header, crc)

	n, err := w.dest.Write(header)
	if err != nil {
		return err
	}
	util.Assert(n == len(header)) // todo

	n, err = w.dest.Write(record)
	if err != nil {
		return err
	}
	util.Assert(n == len(record)) // todo
	return w.dest.Sync()
}
