package skiplist

type Iterator struct {
	list *Skiplist
	p    *Node
}

func (it *Iterator) Next() {
	if it.p != nil {
		it.p = it.p.Next(0)
	}
}

func (it *Iterator) Valid() bool {
	return it.p != nil
}

func (it *Iterator) Key() Key {
	return it.p.Key()
}
