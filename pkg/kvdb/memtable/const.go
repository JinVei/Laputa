package memtable

type ValueType uint8

const (
	KTypeValue  = 0x01
	KTypeDelete = 0x02
)
