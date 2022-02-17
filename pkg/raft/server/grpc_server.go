package server

import (
	raftPb "Laputa/api/raft/v1alpha1"
	"Laputa/pkg/utils/log"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	RaftRoleFollower  = 0
	RaftRoleCandidate = iota
	RaftRoleLeader    = iota
)

type Config struct {
	host string
	port int
}

type Server struct {
	state *RaftState
	cfg   Config
	wlock sync.Mutex
	raftPb.UnimplementedRaftServer
	peers map[int]*Peer
}

func New() *Server {
	s := &Server{}
	return s
}

func (s *Server) Init() error {
	err := s.state.Recover(nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Run() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.cfg.host, s.cfg.port))
	if err != nil {
		log.Error(err, "Get error in net.Listen()")
		panic(err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	raftPb.RegisterRaftServer(grpcServer, s)
	err = grpcServer.Serve(listener)

	return err
}

func (s *Server) RequestVote(ctx context.Context, req *raftPb.VoteRequest) (*raftPb.VoteReply, error) {
	s.wlock.Lock()
	defer s.wlock.Unlock()

	resp := &raftPb.VoteReply{}

	if req.Term < s.state.currentTerm ||
		req.LastLogTerm < s.state.lastLogTerm ||
		req.LastLogIndex < s.state.lastLogIndex ||
		(0 <= s.state.voteFor && s.state.voteFor != req.CandidateId) {

		resp.Term = s.state.GetCurrentTerm()
		resp.VoteGranted = false
		return resp, nil
	}

	if s.state.currentRole != RaftRoleFollower {
		// todo err
		_ = s.becomeFollower(req.Term)
	}

	s.state.ResetElectTimeout()

	s.state.currentTerm = req.Term
	resp.Term = s.state.currentTerm
	resp.VoteGranted = true
	return resp, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftPb.AppendEntriesRequest) (*raftPb.AppendEntriesReply, error) {
	s.wlock.Lock()
	defer s.wlock.Unlock()

	makeFailResp := func() (*raftPb.AppendEntriesReply, error) {
		resp := &raftPb.AppendEntriesReply{}
		resp.Term = s.state.currentTerm
		resp.Success = false
		return resp, nil
	}
	makeSuccessResp := func() (*raftPb.AppendEntriesReply, error) {
		resp := &raftPb.AppendEntriesReply{}
		resp.Term = s.state.currentTerm
		resp.Success = true
		return resp, nil
	}

	if req.Term < s.state.currentTerm || s.state.lastLogIndex < req.PrevLogIndex {
		return makeFailResp()
	}

	s.state.ResetElectTimeout()

	if s.state.currentRole != RaftRoleFollower {
		// todo err
		_ = s.becomeFollower(req.Term)
	}

	entrys := req.Entries

	if req.PrevLogIndex < s.state.lastLogIndex {
		var err error
		entrys, err = s.resolveConflictEntry(entrys, req.PrevLogTerm, req.PrevLogIndex)
		if err != nil {
			return makeFailResp()
		}
	}

	err := s.state.AppendEntrys(entrys)
	if err != nil {
		log.Error(err, "Get error in s.state.AppendEntrys(req.Entries)")
		return makeFailResp()
	}

	if err := s.state.SetCommitIndex(req.LeaderCommit); err != nil {
		log.Error(err, "Get error in s.state.SetCommitIndex(req.LeaderCommit)")
	}

	return makeSuccessResp()
}

func (s *Server) InstallSnapshot(context.Context, *raftPb.InstallSnapshotRequest) (*raftPb.InstallSnapshotReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}

func (s *Server) resolveConflictEntry(entries []*raftPb.Entry, prevLogTerm, prevLogIndex uint64) ([]*raftPb.Entry, error) {
	matchNum := 0
	num, err := s.state.MatchEntryAt(prevLogTerm, prevLogIndex, entries)
	if err != nil {
		log.Error(err, "Get error in s.state.MatchEntryAt(req.PrevLogTerm, req.PrevLogIndex, req.Entries)")
		return nil, err
	}
	matchNum = num

	// There is no entry matches PrevLogTerm and PrevLogIndex.
	if matchNum < 0 {
		return nil, errors.New("There is no entry matches PrevLogTerm and PrevLogIndex.")
	}

	if len(entries) <= matchNum {
		return []*raftPb.Entry{}, nil
	}

	if matchNum < len(entries) {
		// Mismatch some entry. Should replace all mismatching entry with leader's entrys
		if uint64(matchNum)+prevLogIndex < s.state.lastLogIndex {
			if err := s.state.TruncateEntrys(uint64(matchNum) + prevLogIndex); err != nil {
				log.Error(err, "Get error in state.TruncateEntrys()")
				return nil, err
			}
		}
	}
	return entries[matchNum:], nil

}

func (s *Server) becomeFollower(term uint64) error {
	return nil
}

func (s *Server) becomeCandidate() error {
	return nil
}

func (s *Server) becomeLeader() error {
	return nil
}

func (s *Server) runElectionTimer() {
	timeoutDuration := s.state.electTimeout
	s.wlock.Lock()
	termStarted := s.state.currentTerm
	s.wlock.Unlock()
	log.Info("Run election timer", "timeout", timeoutDuration, "term", termStarted)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		if !s.checkElection(termStarted) {
			return
		}
	}
}

func (s *Server) checkElection(term uint64) bool {
	s.wlock.Lock()
	defer s.wlock.Unlock()

	if s.state.currentRole != RaftRoleCandidate && s.state.currentRole != RaftRoleFollower {
		log.Info("stop election ticker becase state change", "term", term, "state", s.state.currentRole)
		return false
	}

	if term != s.state.currentTerm {
		log.Info("stop election ticker becase term stale", "term", term, "state", s.state.currentRole)
		return false
	}

	if elapsed := time.Since(s.state.electLastReset); elapsed >= s.state.electTimeout {
		s.startElection()
		return false
	}
	return true
}

func (s *Server) startElection() {
	s.state.currentRole = RaftRoleCandidate
	s.state.currentTerm += 1
	voteTerm := s.state.currentTerm
	s.state.ResetElectTimeout()
	s.state.voteFor = s.state.id
	VoteGrantedCnt := 1
	for _, p := range s.peers {
		go func(peer *Peer) {
			req := &raftPb.VoteRequest{
				Term:         voteTerm,
				CandidateId:  s.state.id,
				LastLogIndex: s.state.lastLogIndex,
				LastLogTerm:  s.state.lastLogTerm,
			}
			reply, err := peer.RaftClient.RequestVote(context.TODO(), req)
			if err != nil {
				log.Error(err, "Error from p.RaftClient.RequestVote()")
				return
			}
			s.wlock.Lock()
			defer s.wlock.Unlock()
			if reply.Term > voteTerm {
				log.Info("term out of date in RequestVoteReply", "reply.Term", reply.Term)
				// tode: check err
				_ = s.becomeFollower(reply.Term)
				return
			} else if reply.VoteGranted && reply.Term == voteTerm {
				VoteGrantedCnt++
				if VoteGrantedCnt*2 > len(s.peers)+1 {
					log.Info("Wins election with %d votes", "votes cnt", VoteGrantedCnt)
					s.startLeader()
					return
				}

			}

		}(p)
	}

	go s.runElectionTimer()
}

func (s *Server) startLeader() {
}