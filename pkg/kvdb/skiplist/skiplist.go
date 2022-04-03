package skiplist

import (
	"Laputa/pkg/utils/random"
	"time"
)

const (
	KMaxHeight = 12
)

type Key interface{}

type Skiplist struct {
	head      *Node
	compare   Comparator
	maxHeight int
	rand      *random.Random
}

/*
if k1 < k2,  ret < 0 (+1)
if k1 == k2, ret == 0
if k1 > k2,  0 < ret (-1)
*/
type Comparator func(k1, k2 interface{}) int

func New(cmp Comparator) *Skiplist {
	l := &Skiplist{}
	l.head = NewNode(emptyKey{}, KMaxHeight)
	l.maxHeight = 1
	l.rand = random.New(uint32(time.Now().Unix()))
	l.compare = cmp
	return l
}

func NewNode(k Key, height int) *Node {
	x := &Node{}
	x.key = k
	x.next = make([]*Node, height, height)
	return x
}

func (l *Skiplist) Iterator() *Iterator {
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
		if next != nil && l.compare(next.Key(), key) <= 0 {
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
		if next != nil && l.compare(next.Key(), key) < 0 {
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
	if x.next[0] != nil && l.compare(x.next[0].Key(), k) == 0 {
		return x.next[0]
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

func (k emptyKey) Instance() Key {
	return k
}
