package server

import (
	raftPb "Laputa/api/raft/v1alpha1"
	"Laputa/pkg/util/log"
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"Laputa/cmd/raft_server/app/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	RaftRoleFollower  = 0
	RaftRoleCandidate = iota
	RaftRoleLeader    = iota
)

type Server struct {
	state      *RaftState
	listenAddr string
	wlock      sync.Mutex
	raftPb.UnimplementedRaftServer
	peers map[int]*Peer
}

func New(c *config.Config) (*Server, error) {
	s := &Server{}
	s.peers = make(map[int]*Peer)
	for _, p := range c.InitialCluster {
		if p.ID == c.ID {
			continue
		}

		conn, err := grpc.Dial(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Error(err, "grpc.Dial() error in InitialCluster")
			continue
		}
		c := raftPb.NewRaftClient(conn)
		s.peers[int(p.ID)] = &Peer{
			ID:         int(p.ID),
			Addr:       p.Addr,
			RaftClient: c,
		}
	}

	state, err := newState(c.LogPath, c.ElectTimeout, c.ID)
	if err != nil {
		return nil, err
	}
	s.state = state
	s.listenAddr = c.ListenAddr
	return s, nil
}

func (s *Server) Init() error {
	err := s.state.Recover(nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Run() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		log.Error(err, "Get error in net.Listen()")
		panic(err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	raftPb.RegisterRaftServer(grpcServer, s)
	_ = s.becomeFollower(1)
	err = grpcServer.Serve(listener)

	return err
}

func (s *Server) Close() error {
	return nil
}

func (s *Server) RequestVote(ctx context.Context, req *raftPb.VoteRequest) (*raftPb.VoteReply, error) {
	s.wlock.Lock()
	defer s.wlock.Unlock()
	log.Info("RequestVote", "req.term", req.Term, "req.CandidateId", req.CandidateId)
	resp := &raftPb.VoteReply{}

	if req.Term < s.state.currentTerm ||
		req.LastLogTerm < s.state.lastLogTerm ||
		req.LastLogIndex < s.state.lastLogIndex {

		resp.Term = s.state.GetCurrentTerm()
		resp.VoteGranted = false
		if s.state.currentTerm < req.Term {
			s.updateTerm(req.Term)
		}

		return resp, nil
	}

	if s.state.currentRole != RaftRoleFollower {
		// todo err
		_ = s.becomeFollower(req.Term)
	}

	s.state.ResetElectTimeout()

	s.updateTerm(req.Term)
	s.state.voteFor = req.CandidateId
	resp.Term = s.state.currentTerm
	resp.VoteGranted = true
	return resp, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftPb.AppendEntriesRequest) (*raftPb.AppendEntriesReply, error) {
	s.wlock.Lock()
	defer s.wlock.Unlock()

	log.Info("AppendEntries", "req.term", req.Term, "req.LeaderId", req.LeaderId)

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

	if s.state.currentRole != RaftRoleFollower {
		// todo err
		_ = s.becomeFollower(req.Term)
	}

	s.state.ResetElectTimeout()

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
	log.Info("Become Follewer", "term", term)
	s.state.currentRole = RaftRoleFollower
	//s.state.voteFor = -1
	s.state.electLastReset = time.Now()

	s.updateTerm(term)
	return nil
}

func (s *Server) updateTerm(term uint64) {
	if s.state.currentTerm < term {
		s.state.currentTerm = term
		go s.runElectionTimer()
	}
}

func (s *Server) runElectionTimer() {
	timeoutDuration := s.state.electTimeout
	s.wlock.Lock()
	termStarted := s.state.currentTerm
	s.wlock.Unlock()
	log.Info("Run election timer", "timeout", timeoutDuration, "term", termStarted)
	ticker := time.NewTicker(2 * time.Second)
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

	if term < s.state.currentTerm {
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
	s.state.currentRole = RaftRoleLeader

	log.Info("Becomes Leader", "term", s.state.currentTerm)
	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)
		defer ticker.Stop()

		for {
			s.leaderSendHeartbeats()
			<-ticker.C

			s.wlock.Lock()
			if s.state.currentRole != RaftRoleLeader {
				s.wlock.Unlock()
				return
			}
			s.wlock.Unlock()
		}
	}()
}

func (s *Server) leaderSendHeartbeats() {
	s.wlock.Lock()
	term := s.state.currentTerm
	s.wlock.Unlock()

	for _, p := range s.peers {
		// append entry
		req := &raftPb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     s.state.id,
			PrevLogIndex: p.PrevLogIdx,
			PrevLogTerm:  p.PrevLogTerm,
		}
		go func(p *Peer) {
			log.Info("Sending Heartbeats", "leader id", s.state.id, "peer id", p.ID)
			reply, err := p.AppendEntries(context.TODO(), req)
			if err != nil {
				log.Error(err, "Sending Heartbeats error")
				return
			}
			s.wlock.Lock()
			defer s.wlock.Unlock()
			if reply.Term > term {
				log.Info("term out of date in heartbeat reply", "term", term)
				_ = s.becomeFollower(reply.Term)
				return
			}
			// todo
			// if !reply.Success {
			// 	p.PrevLogIdx
			// }
		}(p)
	}
}
