package table

import (
	"Laputa/pkg/kvdb/common"
	"sort"
)

type MergingIterator struct {
	iters   []*Iterator
	next    *Iterator
	compare common.Compare
	valid   bool
}

func NewMergingIterator(iters []*Iterator, cmp common.Compare) *MergingIterator {
	miter := &MergingIterator{}
	miter.iters = iters
	miter.compare = cmp
	miter.valid = true
	miter.Next()
	return miter
}

func (miter *MergingIterator) SeekToFirst() {
	for _, iter := range miter.iters {
		iter.SeekToFirst()
	}
}

func (miter *MergingIterator) Next() {
	if miter.next != nil && miter.next.Valid() {
		miter.next.Next()
	}
	iterSort := SortIterator{
		iters:   miter.iters,
		compare: miter.compare,
	}
	sort.Sort(iterSort) // TODO: performance improve
	miter.next = miter.iters[0]
	if !miter.next.Valid() {
		miter.valid = false
		return
	}
}

func (miter *MergingIterator) Valid() bool {
	return miter.valid
}

func (miter *MergingIterator) Seek(target []byte) {
	for _, iter := range miter.iters {
		iter.Seek(target)
		if iter.Valid() {
			miter.next = iter
			return
		}
	}
}

func (miter *MergingIterator) Key() []byte {
	return miter.next.Key()
}

func (miter *MergingIterator) Value() []byte {
	return miter.next.Value()
}

type SortIterator struct {
	compare common.Compare
	iters   []*Iterator
}

func (s SortIterator) Len() int {
	return len(s.iters)
}

func (s SortIterator) Swap(i, j int) {
	s.iters[i], s.iters[j] = s.iters[j], s.iters[i]
}

func (s SortIterator) Less(i, j int) bool {
	if !s.iters[i].Valid() {
		return false
	}
	if !s.iters[j].Valid() {
		return true
	}
	cmp := s.compare(s.iters[i].Key(), s.iters[j].Key())
	if cmp < 0 {
		return true
	}
	return false
}
