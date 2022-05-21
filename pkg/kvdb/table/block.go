package table

import (
	"Laputa/pkg/kvdb/common"
	"bytes"
	"encoding/binary"
)

type BlockContent []byte

type BlockIterator struct {
	block         BlockContent
	blkoffset     int
	key           *bytes.Buffer
	value         *bytes.Buffer
	entry         *Entry
	restarts      []uint32
	numRestarts   uint32
	compare       common.Compare
	endDataOffset int
}

func NewBlockIterator(block BlockContent, opts *common.Options) *BlockIterator {
	iter := &BlockIterator{}
	iter.compare = opts.KeyComparator
	if iter.compare == nil {
		iter.compare = bytes.Compare
	}

	iter.block = block
	iter.blkoffset = 0
	iter.key = bytes.NewBuffer(make([]byte, 0, 5))
	iter.value = bytes.NewBuffer(make([]byte, 0, 5))
	iter.entry = NewEntry()

	iter.initRestarts()

	iter.Next()

	return iter
}

func (iter *BlockIterator) Next() {
	if iter.blkoffset == iter.endDataOffset {
		iter.blkoffset++ // make iter invalid
	}

	if !iter.Valid() {
		return
	}

	n := iter.entry.DecodeFrom(iter.block, iter.blkoffset)
	iter.blkoffset += n

	iter.key.Truncate(iter.entry.Shard())
	iter.key.Write(iter.entry.DeltaKey())

	iter.value.Reset()
	iter.value.Write(iter.entry.Value())
}

func (iter *BlockIterator) Valid() bool {
	return iter.blkoffset <= iter.endDataOffset
}

func (iter *BlockIterator) Key() common.InternalKey {
	return iter.key.Bytes()
}

func (iter *BlockIterator) Value() []byte {
	return iter.value.Bytes()
}

func (iter *BlockIterator) SeekToFirst() {
	iter.seekTo(int(iter.restarts[0]))
}

func (iter *BlockIterator) SeekToLast() {
	iter.seekTo(int(iter.restarts[iter.numRestarts-1]))
	for iter.blkoffset < iter.endDataOffset {
		iter.Next()
	}
}

func (iter *BlockIterator) seekTo(offset int) {
	iter.blkoffset = offset
	iter.key.Reset()
	iter.key.Reset()

	if !iter.Valid() {
		return
	}
	iter.Next()
}

// return which restart point nearest
func (iter *BlockIterator) SeektoNearest(target []byte) (left, right int) {
	left, right, mid := int(0), int(iter.numRestarts-1), int(0)

	// And also break Loop when shrink search area into one Restart-Point
	for left < right && 1 < right-left {
		mid = (left + right) / 2
		iter.seekTo(int(iter.restarts[mid]))
		cmpRes := iter.compare(iter.Key().UserKey(), target)
		if 0 < cmpRes {
			right = mid
		} else if cmpRes < 0 {
			left = mid
		} else {
			left = mid
			break
		}
	}
	return
}

// if found target key, iter.Valid() == true, or iter.Valid() == false
func (iter *BlockIterator) Seek(target []byte) {
	if iter == nil {
		panic("iter is nil")
	}
	left, right := iter.SeektoNearest(target)

	rightBoundary := right + 1
	hasRightBoundary := uint32(rightBoundary) < iter.numRestarts

	if hasRightBoundary {
		iter.seekTo(int(iter.restarts[rightBoundary]))
		if iter.compare(iter.Key().UserKey(), target) < 0 {
			// target large than right key.
			// in this case mean cant find target key in block
			iter.blkoffset = len(iter.block) // make iter invalid
			return
		}
	}

	iter.seekTo(int(iter.restarts[left]))

	for {
		cmpRes := iter.compare(iter.Key().UserKey(), target)
		if cmpRes == 0 {
			return
		} else if cmpRes < 0 && iter.Valid() && (!hasRightBoundary || iter.blkoffset <= int(iter.restarts[rightBoundary])) {
			//  iter.Key() < target
			iter.Next()
		} else {
			// target < iter.Key() or iter has run out
			// in this case, mean cant find target key in block
			iter.blkoffset = len(iter.block) // make iter invalid
			return
		}
	}

}

func (iter *BlockIterator) initRestarts() {
	offset := uint32(len(iter.block) - 4)
	iter.numRestarts = binary.LittleEndian.Uint32(iter.block[offset : offset+4])
	offset -= iter.numRestarts * 4

	iter.restarts = iter.restarts[0:0]
	for i := uint32(0); i < iter.numRestarts; i++ {
		iter.restarts = append(iter.restarts, binary.LittleEndian.Uint32(iter.block[offset+i*4:offset+i*4+4]))
	}
	iter.endDataOffset = int(offset)
}

func (iter *BlockIterator) ResetContent(block BlockContent) {

	iter.block = block
	iter.blkoffset = 0
	iter.key.Reset()
	iter.value.Reset()
	iter.initRestarts()

	iter.Next()
}
