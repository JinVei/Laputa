package memtable

import (
	"bytes"
	"fmt"
	"testing"

	"gotest.tools/assert"
)

func TestMemtable(t *testing.T) {
	mtable := New(nil)
	keyone := []byte("hi")
	valone := []byte("jo")
	keytwo := []byte("bug-less")
	valtwo := []byte("long-time-ago")
	seq := uint64(1)
	mtable.Put(seq, KTypeValue, keyone, valone) // seq 1
	seq++
	mtable.Put(2, KTypeDelete, keyone, valone) // seq 2
	seq++

	lk := LookupKey{}

	lk.Sequence = 1
	lk.Key = keyone
	fval, foud := mtable.Get(lk)
	assert.Assert(t, foud)
	assert.Assert(t, fval != nil)
	assert.Assert(t, bytes.Compare(fval, valone) == 0)

	lk.Sequence = 2
	fval, foud = mtable.Get(lk)
	assert.Assert(t, !foud)
	assert.Assert(t, fval == nil)

	// put new
	mtable.Put(seq, KTypeValue, keyone, valone) // seq = 3
	seq++
	mtable.Put(seq, KTypeValue, keytwo, valtwo) // seq = 4
	seq++

	lk.Sequence = seq

	lk.Key = keyone
	fval, foud = mtable.Get(lk)
	assert.Assert(t, foud)
	assert.Assert(t, fval != nil)
	assert.Assert(t, bytes.Compare(fval, valone) == 0)

	lk.Key = keytwo
	fval, foud = mtable.Get(lk)
	assert.Assert(t, foud)
	assert.Assert(t, fval != nil)
	assert.Assert(t, bytes.Compare(fval, valtwo) == 0)

	// change value of keyone to valtwo
	lk.Key = keyone
	//lk.Sequence = 5
	mtable.Put(seq, KTypeValue, keyone, valtwo)
	seq++
	assert.Assert(t, foud)
	assert.Assert(t, fval != nil)
	assert.Assert(t, bytes.Compare(fval, valtwo) == 0)

	// try to get snapshot value
	lk.Key = keyone
	lk.Sequence = 3
	fval, foud = mtable.Get(lk)
	assert.Assert(t, foud)
	assert.Assert(t, fval != nil)
	assert.Assert(t, bytes.Compare(fval, valone) == 0)
}

func BenchmarkMemtablePut(b *testing.B) {
	key := []byte("im key")
	value := []byte("im value")

	mtable := New(nil)
	for i := 0; i < b.N; i++ {
		mtable.Put(uint64(i), KTypeValue, key, value)
	}
	allocatedKB := mtable.allocated / 1024
	fmt.Printf("memtable memery usage: %d KB, b.N=%d\n", allocatedKB, b.N)
}

func BenchmarkMemtablePutGet(b *testing.B) {
	key := []byte("im key")
	value := []byte("im value")

	lk := LookupKey{}
	lk.Key = key

	mtable := New(nil)
	for i := 0; i < b.N; i++ {
		lk.Sequence = uint64(i)
		mtable.Put(uint64(i), KTypeValue, key, value)
	}
	for i := 0; i < b.N; i++ {
		lk.Sequence = uint64(i)
		mtable.Get(lk)
	}
}

func BenchmarkMemtableGet(b *testing.B) {
	key := []byte("im key")
	//value := []byte("im value")
	lk := LookupKey{}
	lk.Key = key
	mtable := New(nil)
	for i := 0; i < b.N; i++ {
		lk.Sequence = uint64(i)
		mtable.Get(lk)
	}
}
