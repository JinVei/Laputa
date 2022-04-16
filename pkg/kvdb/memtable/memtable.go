package memtable

import (
	"Laputa/pkg/kvdb/skiplist"
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"Laputa/pkg/kvdb/common"
)

func New(cmp common.Compare) *Memtable {
	mtable := &Memtable{}
	mtable.userKeyCompare = cmp
	if mtable.userKeyCompare == nil {
		mtable.userKeyCompare = bytes.Compare
	}
	mtable.table = skiplist.New(mtable.memtableKeycompare)
	return mtable
}

type Memtable struct {
	table          *skiplist.Skiplist
	ref            int64
	allocated      uint64
	userKeyCompare common.Compare
}

func (t *Memtable) Ref() {
	atomic.AddInt64(&t.ref, 1)
	t.ref++
}

func (t *Memtable) Unref() {
	atomic.AddInt64(&t.ref, -1)
}

func (t *Memtable) Get(lkey LookupKey) ([]byte, bool) {
	internalKeylen := uint64(len(lkey.Key) + 8)
	varintLen := VarintLen(internalKeylen)
	tag := lkey.Sequence<<8 | uint64(KTypeValue)
	mkey := make([]byte, varintLen+int(internalKeylen))

	keyWriter := bytes.NewBuffer(mkey)
	keyWriter.Reset()
	EncodeMemtableKey(keyWriter, lkey.Key, tag)
	x := t.table.FindLessThan(mkey)
	if x.Next(0) == nil {
		return nil, false
	}
	x = x.Next(0)

	entry := DecodeEntry(x.Key().([]byte))
	if t.userKeyCompare(entry.UserKey(), lkey.Key) != 0 {
		return nil, false
	}

	switch entry.ValueType() {
	case KTypeDelete:
		return nil, false
	case KTypeValue:
		return entry.Value(), true
	}
	return nil, false
}

// make an entry, and put it to table
// entry format:
//                           -------memtable key-------
//  klength    varint32
//                              ----internal key----
//  userkey    char[klength]
//  tag(sequence>>8|type)  uint64
//                               ----internal key---
//                           --------memtable key-------
//  vlength    varint32
//  value      char[vlength]
func (t *Memtable) PutEntry(entry *Entry) {
	t.table.Insert(entry.Bytes())
	t.allocated += uint64(len(entry.Bytes()))
}

func (t *Memtable) Put(seq uint64, vtype ValueType, key []byte, value []byte) {
	mkey := NewMemtableKey(key, seq, vtype)
	valLen := len(value)
	varintLen := VarintLen(uint64(valLen))
	mkeyLen := len(mkey)

	entry := make([]byte, mkeyLen+varintLen+valLen)
	offset := 0
	// put memtable key
	copy(entry, mkey)
	offset += mkeyLen

	// put value len
	binary.PutVarint(entry[mkeyLen:], int64(valLen))
	offset += varintLen

	// put value
	copy(entry[offset:], value)
	offset += valLen

	t.table.Insert(entry)
	t.allocated += uint64(offset)
}

func (t *Memtable) MemoryUsage() uint64 {
	return t.allocated
}

// default increasing by User Key, then decreasing by Sequence
func (t *Memtable) memtableKeycompare(k1, k2 interface{}) int {
	entry1 := DecodeEntry(k1.([]byte))
	entry2 := DecodeEntry(k2.([]byte))
	w := 0
	w = t.userKeyCompare(entry1.UserKey(), entry2.UserKey())

	if w == 0 {
		return int(entry2.Sequence()) - int(entry1.Sequence())
	}
	return w
}

func (t *Memtable) Iterator() *Iterator {
	return &Iterator{
		listIter: t.table.Iterator(),
	}
}

type Iterator struct {
	listIter *skiplist.Iterator
}

func (iter *Iterator) Next() {
	iter.listIter.Next()
}

func (iter *Iterator) Valid() bool {
	return iter.listIter.Valid()
}

func (iter *Iterator) Get() Entry {
	raw := iter.listIter.Key()
	entry := DecodeEntry(raw.([]byte))
	return entry
}
