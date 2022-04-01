package skiplist

type Node struct {
	key  Key
	next []*Node
}

func (o *Node) Key() Key {
	return o.key
}

func (o *Node) Next(level int) *Node {
	if len(o.next) < level {
		return nil
	}
	return o.next[level]
}

func (o *Node) SetNext(level int, x *Node) {
	if len(o.next) <= level {
		panic("panic in Node.SetNext")
	}
	o.next[level] = x
}
