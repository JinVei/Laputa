package skiplist

import (
	"math/rand"
)

type Key interface {
	//Hash() int
	// ret < 0 less than
	// ret == 0 equal
	// ret > 0 greater than
	Compare(k1 Key) int
	// get the real instance of the 'key' obj
	Instance() interface{}
}

type ISkiplist interface {
	Insert(key Key)
	Contains(key Key) bool
	FindGreaterOrEqual(key Key) (node *Node, prev []*Node)
	FindLessThan(key Key) *Node
	Iterator() IIterator
	Find(k Key) *Node
}

type Skiplist struct {
	head      *Node
	compare   Comparator
	maxHeight int
}

type Comparator func(k1 []byte, k2 []byte) int

func New() ISkiplist {
	l := &Skiplist{}
	l.head = NewNode(emptyKey{}, KMaxHeight)
	l.maxHeight = 1
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

func (l *Skiplist) FindGreaterOrEqual(key Key) (*Node, []*Node) {
	prev := make([]*Node, l.GetMaxHeight())
	x := l.head
	copy(prev, l.head.next)
	level := l.GetMaxHeight() - 1

	for {
		next := x.Next(level)
		if next != nil && next.key.Compare(key) < 0 {
			x = next
			continue
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
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

func getRandomHeight() int {
	StepProbability := 4 // 1/4
	height := 1
	// random height
	for height < KMaxHeight && rand.Int()%StepProbability == 0 {
		height++
	}
	return height
}

func (l *Skiplist) Insert(key Key) {
	_, prev := l.FindGreaterOrEqual(key)

	height := getRandomHeight()
	o := NewNode(key, height)
	if l.maxHeight < height {
		for i := l.maxHeight; i < height; i++ {
			prev = append(prev, l.head)
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
