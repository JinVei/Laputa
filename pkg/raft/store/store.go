package store

type IStateMachine interface {
	Apply(idx uint64, data []byte) error
	GetLastApplied() uint64
	StoreLastApplied(uint64) error
}

type StateMachine struct {
	lastApplied uint64
	dat         []string
}

func NewDemoStateMachine() *StateMachine {
	return &StateMachine{}
}

func (sm *StateMachine) GetLastApplied() uint64 {
	return sm.lastApplied
}

func (sm *StateMachine) StoreLastApplied(idx uint64) error {
	sm.lastApplied = idx
	return nil
}

func (sm *StateMachine) Apply(idx uint64, data []byte) error {
	sm.dat = append(sm.dat, string(data))
	sm.lastApplied = idx
	return nil
}
