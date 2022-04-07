package table

import (
	"Laputa/pkg/kvdb/common"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/golang/snappy"
)

var ()

type TableBuilder struct {
	file         *os.File
	opts         *common.Options
	lastKey      bytes.Buffer
	datakBuilder *BlockBuilder
	indexBuilder *BlockBuilder
	currIndex    BlockHandle
	snappybuf    []byte
}

func NewTableBuilder(file *os.File, opts *common.Options) (*TableBuilder, error) {
	tb := &TableBuilder{}
	tb.file = file
	offset, err := tb.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	tb.currIndex.Offset = int64(offset)

	tb.opts = opts
	tb.lastKey = *bytes.NewBuffer(make([]byte, 5))
	tb.datakBuilder = NewBlockBuilder(opts.RestartInterval)
	tb.indexBuilder = NewBlockBuilder(opts.IndexRestartInterval)
	tb.snappybuf = make([]byte, 0)

	return tb, nil
}

func (builder *TableBuilder) Add(key, value []byte) error {
	// key must larger than lastKey
	if 0 < builder.opts.KeyComparator(builder.lastKey.Bytes(), key) {
		return ErrUnorderKey
	}

	builder.datakBuilder.Add(key, value)

	builder.lastKey.Reset()
	builder.lastKey.Write(key)

	if builder.opts.TableBlockSize <= builder.datakBuilder.EstimatedSize() {
		if err := builder.flushDataBuilder(); err != nil {
			return err
		}
	}
	return nil
}

func (builder *TableBuilder) Finish() error {
	if err := builder.flushDataBuilder(); err != nil {
		return err
	}
	footer := &Footer{}
	offset := builder.currIndex.Offset

	// write index block
	n, err := builder.writeBlock(builder.indexBuilder.Finish(), false)
	if err != nil {
		return err
	}
	footer.IndexHandle.Offset = offset
	footer.IndexHandle.Size = int64(n)
	offset = offset + footer.IndexHandle.Size

	// TODO: write meta index block
	footer.MetaIndexHandle.Offset = offset
	footer.MetaIndexHandle.Size = 0

	footerBytes := make([]byte, FooterEncodedLen, FooterEncodedLen)
	footer.EncodeTo(footerBytes)

	_, err = builder.file.Write(footerBytes)
	if err != nil {
		return err
	}

	return nil
}

func (builder *TableBuilder) flushDataBuilder() error {
	if builder.datakBuilder.Empty() {
		return nil
	}

	block := builder.datakBuilder.Finish()

	n, err := builder.writeBlock(block, builder.opts.CompressTableBlock)
	if err != nil {
		return err
	}

	builder.currIndex.Size = int64(n)
	indexBytes := make([]byte, 20, 20)
	builder.currIndex.EncodeTo(indexBytes)
	builder.indexBuilder.Add(builder.lastKey.Bytes(), indexBytes)

	// update Offset for next building
	builder.currIndex.Offset += builder.currIndex.Size

	builder.datakBuilder.Reset()

	return nil
}

func (builder *TableBuilder) writeBlock(block []byte, comressed bool) (int, error) {
	compressType := CompressTypeNo
	if comressed {
		// try snappy compress
		n := snappy.MaxEncodedLen(len(block))
		if n < 0 {
			return 0, snappy.ErrTooLarge
		}
		// try grow snappy buffer
		if len(builder.snappybuf) < n {
			builder.snappybuf = make([]byte, n)
		}

		blockCompressed := snappy.Encode(builder.snappybuf, block)
		// not compressed if inefficient
		if len(blockCompressed) < int(0.85*float64((len(block)))) {
			block = blockCompressed
			compressType = CompressTypeSnappy
		}
	}

	// BlockTrailerSize(1-byte type + 32-bit crc)
	crc := crc32.ChecksumIEEE(block)
	trailer := make([]byte, 5, 5)
	trailer[0] = byte(compressType)
	binary.LittleEndian.PutUint32(trailer[1:], crc)

	if n, err := builder.file.Write(block); err != nil {
		return n, err
	}

	if n, err := builder.file.Write(trailer); err != nil {
		return n, err
	}

	builder.file.Sync()

	return len(block) + len(trailer), nil
}
