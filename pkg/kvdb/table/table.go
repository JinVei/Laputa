package table

import (
	"Laputa/pkg/kvdb/common"
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/golang/snappy"
)

type Table struct {
	indexBlock BlockContent
	footer     Footer

	file *os.File
	opts *common.Options

	buffer []byte
}

func New(opts *common.Options) *Table {
	t := &Table{}
	t.opts = opts
	if t.opts == nil {
		t.opts = common.NewDefaultOptions()
	}
	return t
}

func (t *Table) Open(file *os.File) error {
	t.file = file
	_, err := t.file.Seek(int64(-1*FooterEncodedLen), os.SEEK_END)
	if err != nil {
		return err
	}

	t.buffer = tryGrowBytesSlice(t.buffer, FooterEncodedLen)
	n, err := t.file.Read(t.buffer[:FooterEncodedLen])
	if err != nil {
		return err
	}
	t.footer.DecodeFrom(t.buffer[:n])

	err = t.initIndexBlockIter()

	return err

}

func (t *Table) GetFD() *os.File {
	return t.file
}

func (t *Table) initIndexBlockIter() error {
	t.buffer = tryGrowBytesSlice(t.buffer, int(t.footer.IndexHandle.Size))
	n, err := t.file.ReadAt(t.buffer[:t.footer.IndexHandle.Size], t.footer.IndexHandle.Offset)
	if err != nil {
		return err
	}
	indexBlock, _, err := DecodeBlock(nil, t.buffer[:n])
	if err != nil {
		return err
	}
	t.indexBlock = indexBlock
	return nil
}

func (t *Table) NewIterator() *Iterator {
	iter := &Iterator{}
	iter.table = t
	iter.indexIter = NewBlockIterator(t.indexBlock, t.opts)
	iter.Next()
	return iter
}

func tryGrowBytesSlice(slice []byte, n int) []byte {
	if len(slice) < n {
		return make([]byte, n)
	}
	return slice
}

func DecodeBlock(dst, src []byte) ([]byte, int, error) {
	trailer := src[len(src)-5:]
	cType := trailer[0]
	crc := binary.LittleEndian.Uint32(trailer[1:5])
	block := src[:len(src)-5]
	dtmp := dst
	dlen := 0
	var err error

	if crc32.ChecksumIEEE(block) != crc {
		return dst, 0, ErrTableBlockCorruption
	}

	if cType == byte(CompressTypeSnappy) {
		dtmp, err = snappy.Decode(dtmp, src)
		if err != nil {
			return dst, 0, err
		}
	} else {
		if len(dtmp) < len(block) {
			dtmp = make([]byte, len(block), len(block))
		} else {
			dtmp = dtmp[:len(block)]
		}
		copy(dtmp, block)
	}
	dlen = len(dtmp)

	if len(dst) < dlen {
		dst = dtmp
	}

	return dst, len(dtmp), nil
}
