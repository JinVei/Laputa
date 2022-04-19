package log

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"gotest.tools/assert"
)

func TestLogWriterAndReader(t *testing.T) {
	recordOne := []byte("record one")
	recordTwo := []byte("record two")
	recordLarge := make([]byte, 64*1024)
	recordFour := []byte("record four")

	wf, err := os.OpenFile("./test_log.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)

	w := NewWriter(wf)

	err = w.AddRecord(recordOne)
	assert.Assert(t, err == nil, err)

	err = w.AddRecord(recordTwo)
	assert.Assert(t, err == nil, err)

	err = w.AddRecord(recordLarge)
	assert.Assert(t, err == nil, err)

	err = w.AddRecord(recordFour)
	assert.Assert(t, err == nil, err)

	rf, err := os.OpenFile("./test_log.log", os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	r := NewReader(rf)

	record, err := r.ReadRecord()
	assert.Assert(t, err == nil, err)
	assert.Assert(t, bytes.Compare(record, recordOne) == 0)

	record, err = r.ReadRecord()
	assert.Assert(t, err == nil, err)
	assert.Assert(t, bytes.Compare(record, recordTwo) == 0)

	record, err = r.ReadRecord()
	assert.Assert(t, err == nil, err)
	assert.Assert(t, bytes.Compare(record, recordLarge) == 0)

	record, err = r.ReadRecord()
	assert.Assert(t, err == nil, err)
	assert.Assert(t, bytes.Compare(record, recordFour) == 0)

	err = os.Remove("./test_log.log")
	if err != nil {
		fmt.Println(err)
	}
}

func TestLogWriterAndReaderAligned(t *testing.T) {
	recordLarge := make([]byte, 32*1024-7)
	recordOne := []byte("record one")

	wf, err := os.OpenFile("./test_log.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(t, err == nil, err)

	w := NewWriter(wf)

	err = w.AddRecord(recordLarge)
	assert.Assert(t, err == nil, err)

	err = w.AddRecord(recordOne)
	assert.Assert(t, err == nil, err)

	rf, err := os.OpenFile("./test_log.log", os.O_RDONLY, 0766)
	assert.Assert(t, err == nil, err)
	r := NewReader(rf)

	record, err := r.ReadRecord()
	assert.Assert(t, err == nil, err)
	assert.Assert(t, bytes.Compare(record, recordLarge) == 0)

	record, err = r.ReadRecord()
	assert.Assert(t, err == nil, err)
	assert.Assert(t, bytes.Compare(record, recordOne) == 0)

	err = os.Remove("./test_log.log")
	if err != nil {
		fmt.Println(err)
	}

}

func BenchmarkAppendLog(b *testing.B) {
	recordOne := []byte("record one")

	wf, err := os.OpenFile("./test_log.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	assert.Assert(b, err == nil, err)

	w := NewWriter(wf)
	for i := 0; i < b.N; i++ {
		err = w.AddRecord(recordOne)
		assert.Assert(b, err == nil, err)
	}

	err = os.Remove("./test_log.log")
	if err != nil {
		fmt.Println(err)
	}
}
