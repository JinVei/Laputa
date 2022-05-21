package table

import (
	"Laputa/pkg/kvdb/common"

	"Laputa/pkg/util"
)

type Iterator struct {
	dataIter  *BlockIterator
	indexIter *BlockIterator
	table     *Table
	valid     bool
	buffer    []byte
	encodebuf []byte
	err       error
}

func (iter *Iterator) Next() {
	if !iter.indexIter.Valid() {
		return
	}

	if iter.dataIter == nil {
		iter.valid = true
		iter.indexIter.SeekToFirst()
		iter.reloadDataBlock()
	} else if !iter.dataIter.Valid() {
		goto NEXT_BLOCK
	} else {
		iter.dataIter.Next()
		if !iter.dataIter.Valid() {
			goto NEXT_BLOCK
		}
	}
	return
NEXT_BLOCK:
	iter.valid = true
	iter.indexIter.Next()
	iter.reloadDataBlock()
}

func (iter *Iterator) Valid() bool {
	return iter.valid
}

func (iter *Iterator) reloadDataBlock() {
	if !iter.indexIter.Valid() {
		iter.valid = false
		return
	}
	//n := 0
	var handle BlockHandle
	handle.DecodeFrom(iter.indexIter.Value())

	iter.encodebuf = tryGrowBytesSlice(iter.encodebuf, int(handle.Size))
	n, err := iter.table.file.ReadAt(iter.encodebuf[:handle.Size], handle.Offset)
	if err != nil {
		goto ERROR
	}

	iter.buffer, n, iter.err = DecodeBlock(iter.buffer, iter.encodebuf[:handle.Size])
	if iter.err != nil {
		goto ERROR
	}

	if iter.dataIter == nil {
		iter.dataIter = NewBlockIterator(iter.buffer[:n], iter.table.opts)
	} else {
		iter.dataIter.ResetContent(iter.buffer[:n])
	}
	return
ERROR:
	iter.valid = false
	iter.err = err
	return
}

func (iter *Iterator) Key() common.InternalKey {
	return iter.dataIter.Key()
}

func (iter *Iterator) Value() []byte {
	return iter.dataIter.Value()
}

func (iter *Iterator) SeekToFirst() {
	iter.indexIter.SeekToFirst()
	iter.reloadDataBlock()
}

// If found target key, iter.Valid() == true, or iter.Valid() == false
// key type is userkey
func (iter *Iterator) Seek(key []byte) {
	letf, right := iter.indexIter.SeektoNearest(key)
	for i := letf; i <= right; i++ {
		iter.indexIter.seekTo(int(iter.indexIter.restarts[i]))
		for iter.indexIter.Valid() {
			iter.reloadDataBlock()
			util.AssertWithMsg(iter.err == nil, iter.err)

			iter.dataIter.Seek(key)
			if iter.dataIter.Valid() {
				iter.valid = true
				return
			}
			iter.indexIter.Next()
		}
	}
	iter.valid = false
	return
}
