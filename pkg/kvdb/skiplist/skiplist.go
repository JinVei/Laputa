package skiplist

import (
	"Laputa/pkg/utils/random"
	"time"
)

type Skiplist struct {
	head      *Node
	compare   Comparator
	maxHeight int
	rand      *random.Random
}

type Comparator func(k1 []byte, k2 []byte) int

func New() ISkiplist {
	l := &Skiplist{}
	l.head = NewNode(emptyKey{}, KMaxHeight)
	l.maxHeight = 1
	l.rand = random.New(uint32(time.Now().Unix()))
	return l
}

func NewNode(k Key, height int) *Node {
	x := &Node{}
	x.key = k
	x.next = make([]*Node, height)
	return x
}

func (l *Skiplist) Iterator() IIterator {
	it := &Iterator{}
	it.list = l
	it.p = l.head.Next(0)
	return it
}

func (l *Skiplist) FindGreaterOrEqual(key Key, prev []*Node) *Node {
	x := l.head
	copy(prev, l.head.next)
	level := l.GetMaxHeight() - 1

	for {
		next := x.Next(level)
		if next != nil && next.key.Compare(key) < 0 {
			x = next
			continue
		} else {
			if prev != nil {
				prev[level] = x
			}
			if level == 0 {
				return next
			}
			level--
		}
	}
}

func (l *Skiplist) GetMaxHeight() int {
	return l.maxHeight
}

func (l *Skiplist) FindLessThan(key Key) *Node {
	x := l.head
	level := l.GetMaxHeight() - 1
	for {
		next := x.Next(level)
		if next != nil && 0 <= key.Compare(next.key) {
			x = next
			continue
		}
		if level == 0 {
			return x
		}
		level--
	}
}

func (l *Skiplist) getRandomHeight() int {
	StepProbability := 4 // 1/4
	height := 1
	// random height
	for height < KMaxHeight && l.rand.OneIn(StepProbability) {
		height++
	}
	return height
}

func (l *Skiplist) Insert(key Key) {
	prev := make([]*Node, KMaxHeight)
	_ = l.FindGreaterOrEqual(key, prev)

	height := l.getRandomHeight()
	o := NewNode(key, height)
	if l.maxHeight < height {
		for i := l.maxHeight; i < height; i++ {
			prev[i] = l.head
		}
		l.maxHeight = height
	}

	for i := 0; i < height; i++ {
		o.SetNext(i, prev[i].Next(i))
		prev[i].SetNext(i, o)
	}
}

func (l *Skiplist) Find(k Key) *Node {
	x := l.FindLessThan(k)
	if x != nil && x.Key().Compare(k) == 0 {
		return x
	}
	return nil
}

func (l *Skiplist) Contains(key Key) bool {
	x := l.Find(key)
	if x != nil {
		return true
	}
	return false
}

type emptyKey struct{}

func (k emptyKey) Hash() int {
	return 0
}

func (k emptyKey) Compare(k1 Key) int {
	return -1
}

func (k emptyKey) Instance() interface{} {
	return k
}
