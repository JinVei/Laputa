package table

import (
	"Laputa/pkg/kvdb/common"
	"bytes"
	"strconv"
	"testing"

	"gotest.tools/assert"
)

func testNumberBytesCompare(k1, k2 []byte) int {
	nk1, _ := strconv.Atoi(string(k1))
	nk2, _ := strconv.Atoi(string(k2))
	return nk1 - nk2
}

func TestBlockBuilder(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare
	builder := NewBlockBuilder(opts.RestartInterval)
	for i := 10; i < 500; i++ {
		builder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
	}
	block := builder.Finish()

	iter := NewBlockIterator(block, opts)
	for i := 10; i < 500; i++ {
		assert.Assert(t, bytes.Compare(iter.Key(), []byte(strconv.Itoa(i))) == 0 &&
			bytes.Compare(iter.Value(), []byte(strconv.Itoa(i*2))) == 0)
		if iter.Valid() {
			iter.Next()
		}
	}

	iter.SeekToFirst()
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(10))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(10*2))) == 0)

	iter.Next()
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(11))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(11*2))) == 0)

	iter.SeekToLast()
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(499))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(499*2))) == 0)

	iter.Next()
	assert.Assert(t, !iter.Valid())

	iter.Seek([]byte(strconv.Itoa(99)))
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(99))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(99*2))) == 0)

	iter.Seek([]byte(strconv.Itoa(551)))
	assert.Assert(t, !iter.Valid())

	iter.Seek([]byte(strconv.Itoa(5)))
	assert.Assert(t, !iter.Valid())
}

func TestBlockBuilderA(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare
	builder := NewBlockBuilder(opts.RestartInterval)
	for i := 10; i < 16; i++ {
		builder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
	}
	block := builder.Finish()

	iter := NewBlockIterator(block, opts)
	for i := 10; i < 16; i++ {
		assert.Assert(t, bytes.Compare(iter.Key(), []byte(strconv.Itoa(i))) == 0 &&
			bytes.Compare(iter.Value(), []byte(strconv.Itoa(i*2))) == 0)
		if iter.Valid() {
			iter.Next()
		}
	}

	iter.Seek([]byte(strconv.Itoa(13)))
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(13))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(13*2))) == 0)

	iter.Seek([]byte(strconv.Itoa(551)))
	assert.Assert(t, !iter.Valid())

	iter.Seek([]byte(strconv.Itoa(8)))
	assert.Assert(t, !iter.Valid())
}

func TestBlockBuilderB(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare
	builder := NewBlockBuilder(opts.RestartInterval)
	for i := 10; i < 46; i++ {
		builder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
	}
	block := builder.Finish()

	iter := NewBlockIterator(block, opts)
	for i := 10; i < 46; i++ {
		assert.Assert(t, bytes.Compare(iter.Key(), []byte(strconv.Itoa(i))) == 0 &&
			bytes.Compare(iter.Value(), []byte(strconv.Itoa(i*2))) == 0)
		if iter.Valid() {
			iter.Next()
		}
	}

	iter.Seek([]byte(strconv.Itoa(13)))
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(13))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(13*2))) == 0)

	iter.Seek([]byte(strconv.Itoa(33)))
	assert.Assert(t, iter.Valid() && bytes.Compare(iter.Key(), []byte(strconv.Itoa(33))) == 0 &&
		bytes.Compare(iter.Value(), []byte(strconv.Itoa(33*2))) == 0)

	iter.Seek([]byte(strconv.Itoa(551)))
	assert.Assert(t, !iter.Valid())

	iter.Seek([]byte(strconv.Itoa(8)))
	assert.Assert(t, !iter.Valid())
}
