package skiplist

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type NKey int

func (k NKey) Compare(k1 Key) int {
	nk1, ok1 := k1.Restore().(NKey)

	if !ok1 {
		panic("Panic in Compare")
	}
	v := int(k) - int(nk1)
	if v < 0 {
		return -1
	} else if v == 0 {
		return 0
	} else {
		return 1
	}
}

func (k NKey) Restore() interface{} {
	return k
}

func TestSkiplist(t *testing.T) {
	rand.Seed(time.Now().Unix())
	l := New()
	t.Log("Test Insert")
	l.Insert(NKey(1))
	l.Insert(NKey(2))
	l.Insert(NKey(10))
	l.Insert(NKey(5))
	l.Insert(NKey(2))

	t.Log("Test Find")
	t.Log(l.Find(NKey(8)))
	t.Log(l.Find(NKey(3)))
	t.Log(l.Find(NKey(2)).Key())

	t.Log("Test Contains")
	t.Log(l.Contains(NKey(10)))
	t.Log(l.Contains(NKey(11)))

	//l.FindGreaterOrEqual()
	t.Log("Test Iterator")
	it := l.Iterator()
	for ; it.Valid(); it.Next() {
		t.Log(it.Key())
	}

	t.Log("Test level len")
	for i := 0; i < 10000; i++ {
		l.Insert(NKey(rand.Int()))
	}

	ll := l.(*Skiplist)
	x := ll.head
	levelCnt := make(map[int]int)
	for x != nil {
		//t.Log(len(x.next))
		levelCnt[len(x.next)]++
		x = x.Next(0)
	}
	t.Log(levelCnt)
	fmt.Println("finish!")
}
