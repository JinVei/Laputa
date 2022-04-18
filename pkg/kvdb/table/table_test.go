package table

import (
	"Laputa/pkg/kvdb/common"
	"os"
	"strconv"
	"testing"

	"gotest.tools/assert"
)

func TestTableBuilderAndIterator(t *testing.T) {
	sst1 := "./test.sst"

	wf, err := os.OpenFile(sst1, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)

	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare

	tbuilder := NewTableBuilder(wf, opts)
	//assert.Assert(t, err == nil, err)
	for i := 1000; i < 5000; i++ {
		err = tbuilder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder.Finish()
	assert.Assert(t, err == nil, err)

	rf, err := os.OpenFile(sst1, os.O_RDONLY, 766)

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
	os.Remove(sst1)

}

func TestTableMergingIterator(t *testing.T) {
	sst1 := "./test1.sst"
	sst2 := "./test2.sst"
	sst3 := "./test3.sst"
	wf1, err := os.OpenFile(sst1, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)
	wf2, err := os.OpenFile(sst2, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)
	wf3, err := os.OpenFile(sst3, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)

	opts := common.NewDefaultOptions()
	opts.KeyComparator = testNumberBytesCompare

	tbuilder1 := NewTableBuilder(wf1, opts)
	for i := 1000; i < 2000; i++ {
		ikey := common.NewInternalKey([]byte(strconv.Itoa(i)), uint64(i), common.KTypeValue)
		err = tbuilder1.Add(ikey, []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder1.Finish()
	assert.Assert(t, err == nil, err)

	tbuilder2 := NewTableBuilder(wf2, opts)
	for i := 2000; i < 3500; i++ {
		ikey := common.NewInternalKey([]byte(strconv.Itoa(i)), uint64(i), common.KTypeValue)
		err = tbuilder2.Add(ikey, []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder2.Finish()
	assert.Assert(t, err == nil, err)

	tbuilder3 := NewTableBuilder(wf3, opts)
	for i := 3000; i < 4000; i++ {
		ikey := common.NewInternalKey([]byte(strconv.Itoa(i)), uint64(i), common.KTypeValue)
		err = tbuilder3.Add(ikey, []byte(strconv.Itoa(i*2)))
		assert.Assert(t, err == nil, err)
	}
	err = tbuilder3.Finish()
	assert.Assert(t, err == nil, err)

	table1 := New(opts)
	rf1, err := os.OpenFile(sst1, os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	err = table1.Open(rf1)
	assert.Assert(t, err == nil, err)

	table2 := New(opts)
	rf2, err := os.OpenFile(sst2, os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	err = table2.Open(rf2)
	assert.Assert(t, err == nil, err)

	table3 := New(opts)
	rf3, err := os.OpenFile(sst3, os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	err = table3.Open(rf3)
	assert.Assert(t, err == nil, err)

	i := 1000
	dup := 0
	icompare := common.NewInternalKeyCompare(opts.KeyComparator).Compare
	miter := NewMergingIterator(append([]*Iterator{}, table1.NewIterator(),
		table3.NewIterator(), table2.NewIterator()), icompare)
	for i < 4000 {
		ikey := common.InternalKey(miter.Key())
		assert.Assert(t, opts.KeyComparator(ikey.UserKey(), []byte(strconv.Itoa(i))) == 0 &&
			opts.KeyComparator(miter.Value(), []byte(strconv.Itoa(i*2))) == 0, string(ikey.UserKey())+":"+string([]byte(strconv.Itoa(i))))

		if 3000 <= i && i < 3500 && dup == 0 {
			dup++
		} else {
			dup = 0
			i++
		}
		miter.Next()
	}
	os.Remove(sst1)
	os.Remove(sst2)
	os.Remove(sst3)

}
