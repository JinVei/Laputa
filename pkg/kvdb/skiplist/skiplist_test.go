package skiplist

import (
	"Laputa/pkg/util/random"
	"container/list"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"gotest.tools/assert"
)

type NKey int

func Compare(k1, k2 interface{}) int {
	nk1, _ := k1.(NKey)
	nk2, _ := k2.(NKey)
	v := int(nk1) - int(nk2)
	return v
}

func TestSkiplist(t *testing.T) {
	rand.Seed(time.Now().Unix())
	l := New(Compare)

	l.Insert(NKey(1))
	l.Insert(NKey(2))
	l.Insert(NKey(10))
	l.Insert(NKey(5))
	l.Insert(NKey(2))

	assert.Assert(t, l.Find(NKey(8)) == nil, true)
	assert.Assert(t, l.Find(NKey(3)) == nil, true)
	assert.Assert(t, l.Find(NKey(2)) != nil, true)

	assert.Assert(t, l.Contains(NKey(10)), true)
	assert.Assert(t, l.Contains(NKey(11)) == false, true)

	it := l.Iterator()
	assert.Assert(t, it.Key().(NKey) == NKey(1))
	it.Next()
	assert.Assert(t, it.Key().(NKey) == NKey(2))
	it.Next()
	assert.Assert(t, it.Key().(NKey) == NKey(2))
	it.Next()
	assert.Assert(t, it.Key().(NKey) == NKey(5))
	it.Next()
	assert.Assert(t, it.Key().(NKey) == NKey(10))
	it.Next()
	assert.Assert(t, it.Valid() == false)

	for i := 0; i < 500000; i++ {
		l.Insert(NKey(rand.Int()))
	}

	ll := l
	x := ll.head
	levelCnt := make(map[int]int)
	for x != nil {
		levelCnt[len(x.next)]++
		x = x.Next(0)
	}
	t.Log(levelCnt)
	fmt.Println("finish!")
}

func TestMap(t *testing.T) {
	// levelCnt := make(map[int]int)
	// for i := 0; i < 5000000; i++ {
	// 	levelCnt[i] = i * 2
	// }
	l := list.New()
	for i := 0; i < 5000000; i++ {
		l.PushBack(rand.Int())
	}
	fmt.Println("finish!!!!")
}

func TestSkiplist1(t *testing.T) {
	l := New(Compare)
	random := random.New(uint32(time.Now().Unix()))

	for i := 0; i < 5000000; i++ {
		l.Insert(NKey(random.Next()))
		//l.Insert(NKey(i))
		//random.Next()
		//l.Insert(NKey(rand.Int()))
	}

	fmt.Println("finish!!!")
}

func BenchmarkStdList(b *testing.B) {
	l := list.New()
	//cnt := 5000000
	for i := 0; i < b.N; i++ {
		l.PushBack(rand.Int())
	}
}

func BenchmarkStdMap(b *testing.B) {
	levelCnt := make(map[int]int)
	cnt := b.N
	for i := 0; i < b.N; i++ {
		levelCnt[rand.Int()%cnt] = i * 2
	}
}

func BenchmarkSkiplist(b *testing.B) {
	l := New(Compare)
	//cnt := 5000000
	for i := 0; i < b.N; i++ {
		l.Insert(NKey(rand.Int()))
		//l.Insert(NKey(i))
		//random.Next()
		//l.Insert(NKey(rand.Int()))
	}
}
