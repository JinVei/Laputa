package server

import (
	raftPb "Laputa/api/raft/v1alpha1"
	"Laputa/pkg/journal"
	"errors"
	"time"

	"github.com/gogo/protobuf/proto"
)

type Peer struct {
	ID int
	raftPb.RaftClient
}

// todo: how to recover raft_state from journal
// todo: how to run when raft role turn to leader
type RaftState struct {
	id uint64
	//locker        sync.Mutex
	currentTerm uint64
	voteFor     uint64
	//electTicker time.Ticker

	// recover from journal
	commitIndex  uint64
	lastApplied  uint64
	lastLogIndex uint64
	lastLogTerm  uint64

	electTimeout    time.Duration
	journal         *journal.Journal
	stateMachine    IStateMachine
	commitLogReader *journal.JournalReader
	currentRole     int

	electLastReset time.Time
}

func (s *RaftState) GetCurrentRole() int {
	return s.currentRole
}

func (s *RaftState) SetCurrentRole(role int) {
	s.currentRole = role
}

func (s *RaftState) GetCurrentTerm() uint64 {
	return s.currentTerm
}

func (s *RaftState) ResetElectTimeout() {
	s.electLastReset = time.Now()
}

func (s *RaftState) Recover(fn func(entry *raftPb.Entry)) error {
	var handlerErr error = nil
	handler := func(b []byte) {
		if handlerErr != nil {
			return
		}

		e := &raftPb.Entry{}
		handlerErr = proto.Unmarshal(b, e)
		s.lastLogIndex = e.Index
		s.lastLogTerm = e.Term
		if fn != nil {
			fn(e)
		}
	}

	err := s.journal.InitAndRecover(handler)
	if err != nil {
		return err
	}
	if handlerErr != nil {
		return handlerErr
	}

	err = s.initCommitLogReader()
	if err != nil {
		return err
	}

	s.currentTerm = s.lastLogTerm
	return nil
}

func (s *RaftState) initCommitLogReader() error {
	r, err := s.journal.NewJournalReader()
	if err != nil {
		return err
	}
	s.commitLogReader = r
	err = loopReadEntry(s.commitLogReader, func(e *raftPb.Entry) bool {
		return e.Index < s.commitIndex
	})
	return err
}

func (s *RaftState) AppendEntrys(entrys []*raftPb.Entry) error {
	for _, e := range entrys {
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}

		if err := s.journal.Append(b); err != nil {
			return err
		}
		s.lastLogIndex = e.GetIndex()
		s.lastLogTerm = e.GetTerm()
		s.currentTerm = s.lastLogTerm
	}
	return nil
}

func (s *RaftState) SetCommitIndex(idx uint64) error {
	// if idx <= s.commitIndex {
	// 	return nil
	// }
	s.commitIndex = idx
	var err error
	err1 := loopReadEntry(s.commitLogReader, func(e *raftPb.Entry) bool {
		if s.lastApplied <= s.commitIndex {
			err = s.stateMachine.Apply(e.Index, e.Data)
			if err != nil {
				return false
			}
			s.lastApplied++
			return true
		} else {
			return false
		}
	})
	if err != nil {
		return err
	}
	if err1 != nil {
		return err1
	}

	_ = s.stateMachine.StoreLastApplied(s.lastApplied)
	return nil
}

// Try to match entrys in journal begining at Term and Index.
// Return the number of matching entrys.
// Return negative if nothing matches at Term and Index.
func (s *RaftState) MatchEntryAt(term, index uint64, entrys []*raftPb.Entry) (int, error) {
	jreader, err := s.journal.NewJournalReader()
	if err != nil {
		return -1, err
	}
	defer jreader.Close()

	matchNum := -1
	for !jreader.IsEnd() {
		b, err := jreader.ReadOneRecord()
		if err != nil {
			return -1, err
		}

		e := &raftPb.Entry{}
		err = proto.Unmarshal(b, e)
		if err != nil {
			return -1, err
		}

		if matchNum < 0 {
			if e.GetTerm() == term && e.GetIndex() == index {
				matchNum = 0
			}
			continue
		}

		if matchNum <= len(entrys) {
			break
		}

		if e.Index == entrys[matchNum].Index && e.Term == entrys[matchNum].Term {
			matchNum++
		} else {
			break
		}
	}

	return matchNum, nil
}

// Truncate all entrys behind the entry which LogIndex is 'idx'
func (s *RaftState) TruncateEntrys(idx uint64) error {
	r, err := s.journal.NewJournalReader()
	if err != nil {
		return err
	}
	defer r.Close()

	found := false
	err1 := loopReadEntry(r, func(e *raftPb.Entry) bool {
		if e.Index == idx {
			err = s.journal.Truncate(r.GetOffset())
			if err != nil {
				return false
			}
			s.lastLogIndex = e.Index
			s.lastLogTerm = e.Term
			found = true
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	if err1 != nil {
		return err1
	}
	if !found {
		return errors.New("There is not matching entry")
	}
	return nil
}

func readOneEntry(r *journal.JournalReader) (*raftPb.Entry, error) {
	b, err := r.ReadOneRecord()
	if err != nil {
		return nil, err
	}

	e := &raftPb.Entry{}
	err = proto.Unmarshal(b, e)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func loopReadEntry(r *journal.JournalReader, fn func(e *raftPb.Entry) bool) error {
	for !r.IsEnd() {
		e, err := readOneEntry(r)
		if err != nil {
			return err
		}
		if !fn(e) {
			break
		}
	}
	return nil
}
