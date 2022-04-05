package table

import (
	"bytes"
	"strconv"
	"testing"

	"gotest.tools/assert"
)

/* BUG
1 shard compare
2 iter.valid
3
*/
func numberBytesCompare(k1, k2 []byte) int {
	nk1, _ := strconv.Atoi(string(k1))
	nk2, _ := strconv.Atoi(string(k2))
	return nk1 - nk2
}

func TestBlockBuilder(t *testing.T) {
	builder := NewBlockBuilder()
	for i := 10; i < 500; i++ {
		builder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*2)))
	}
	block := builder.Finish()

	iter := NewBlockIterator(block, numberBytesCompare)
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
