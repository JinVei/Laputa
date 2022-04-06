package table

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

func (iter *Iterator) Key() []byte {
	return iter.dataIter.Key()
}

func (iter *Iterator) Value() []byte {
	return iter.dataIter.Value()
}

func (iter *Iterator) SeekToFirst() {
	iter.indexIter.SeekToFirst()
	iter.reloadDataBlock()
}
