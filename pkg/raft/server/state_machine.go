package server

type IStateMachine interface {
	Apply(idx uint64, data []byte) error
	GetLastApplied() uint64
	StoreLastApplied(uint64) error
}
