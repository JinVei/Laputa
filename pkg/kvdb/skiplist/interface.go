package skiplist

type IIterator interface {
	//Seek(target Key)
	Next()
	//Prev()
	Key() Key
	Valid() bool
	//SeekToFirst()
	//SeekToLast()
}

type Key interface {
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
	FindGreaterOrEqual(key Key, prev []*Node) *Node
	FindLessThan(key Key) *Node
	Iterator() IIterator
	Find(k Key) *Node
}
