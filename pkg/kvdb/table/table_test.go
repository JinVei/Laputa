package table

import (
	"Laputa/pkg/kvdb/common"
	"os"
	"strconv"
	"testing"

	"gotest.tools/assert"
)

func TestTableBuilderAndIterator(t *testing.T) {
	wf, err := os.OpenFile("./test.sst", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)

	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare

	tbuilder, err := NewTableBuilder(wf, opts)
	assert.Assert(t, err == nil, err)
	for i := 1000; i < 5000; i++ {
		err = tbuilder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder.Finish()
	assert.Assert(t, err == nil, err)

	rf, err := os.OpenFile("./test.sst", os.O_RDONLY, 766)

	table := New(opts)
	err = table.Open(rf)
	assert.Assert(t, err == nil, err)

	iter := table.NewIterator()
	for i := 1000; i < 5000; i++ {
		assert.Assert(t, opts.KeyComparator(iter.Key(), []byte(strconv.Itoa(i))) == 0 &&
			opts.KeyComparator(iter.Value(), []byte(strconv.Itoa(i*2))) == 0, string(iter.Key())+":"+string([]byte(strconv.Itoa(i))))
		assert.Assert(t, opts.KeyComparator(iter.Value(), []byte(strconv.Itoa(i*2))) == 0)
		iter.Next()
	}
	assert.Assert(t, !iter.Valid())

	iter.Seek([]byte(strconv.Itoa(2345)))
	assert.Assert(t, opts.KeyComparator(iter.Key(), []byte(strconv.Itoa(2345))) == 0 &&
		opts.KeyComparator(iter.Value(), []byte(strconv.Itoa(2345*2))) == 0, string(iter.Key())+":"+string([]byte(strconv.Itoa(2345))))

	iter.Seek([]byte(strconv.Itoa(4567)))
	assert.Assert(t, opts.KeyComparator(iter.Key(), []byte(strconv.Itoa(4567))) == 0 &&
		opts.KeyComparator(iter.Value(), []byte(strconv.Itoa(4567*2))) == 0, string(iter.Key())+":"+string([]byte(strconv.Itoa(4567))))

	iter.Seek([]byte(strconv.Itoa(678)))
	assert.Assert(t, !iter.Valid())

	iter.Seek([]byte(strconv.Itoa(7888)))
	assert.Assert(t, !iter.Valid())

}

func TestTableMergingIterator(t *testing.T) {
	wf1, err := os.OpenFile("./test1.sst", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)
	wf2, err := os.OpenFile("./test2.sst", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)
	wf3, err := os.OpenFile("./test3.sst", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)

	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare

	tbuilder1, err := NewTableBuilder(wf1, opts)
	assert.Assert(t, err == nil, err)
	for i := 1000; i < 2000; i++ {
		err = tbuilder1.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder1.Finish()
	assert.Assert(t, err == nil, err)

	tbuilder2, err := NewTableBuilder(wf2, opts)
	assert.Assert(t, err == nil, err)
	for i := 2000; i < 3500; i++ {
		err = tbuilder2.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder2.Finish()
	assert.Assert(t, err == nil, err)

	tbuilder3, err := NewTableBuilder(wf3, opts)
	assert.Assert(t, err == nil, err)
	for i := 3000; i < 4000; i++ {
		err = tbuilder3.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder3.Finish()
	assert.Assert(t, err == nil, err)

	table1 := New(opts)
	rf1, err := os.OpenFile("./test1.sst", os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	err = table1.Open(rf1)
	assert.Assert(t, err == nil, err)

	table2 := New(opts)
	rf2, err := os.OpenFile("./test2.sst", os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	err = table2.Open(rf2)
	assert.Assert(t, err == nil, err)

	table3 := New(opts)
	rf3, err := os.OpenFile("./test3.sst", os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	err = table3.Open(rf3)
	assert.Assert(t, err == nil, err)

	i := 1000
	dup := 0
	miter := NewMergingIterator(append([]*Iterator{}, table1.NewIterator(), table3.NewIterator(), table2.NewIterator()), opts)
	for i < 4000 {
		assert.Assert(t, opts.KeyComparator(miter.Key(), []byte(strconv.Itoa(i))) == 0 &&
			opts.KeyComparator(miter.Value(), []byte(strconv.Itoa(i*2))) == 0, string(miter.Key())+":"+string([]byte(strconv.Itoa(i))))

		if 3000 <= i && i < 3500 && dup == 0 {
			dup++
		} else {
			dup = 0
			i++
		}
		miter.Next()
	}

}
