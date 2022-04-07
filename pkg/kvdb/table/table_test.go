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
