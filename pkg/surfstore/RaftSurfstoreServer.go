package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	// added fields
	id          int64    // self id
	peers       []string // peer addresses including self
	commitIndex int64    // index of highest log entry known to be committed
	lastApplied int64    // index of highest log entry applied to the state machine

	// FIXME: leader data structures
	// // only for the leader
	// // for each server, index of the next log entry to send to that server
	// nextIndex []int // initialized to leader last log index + 1
	// // for each server, index of highest log entry known to be replicated on server
	// matchIndex []int // initialized to 0, increasing monotonically

	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

// return ERR_SERVER_CRASHED if the server is crashed
func (s *RaftSurfstore) CheckIsCrashed() error {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return ERR_SERVER_CRASHED
	}
	return nil
}

// return ERR_NOT_LEADER if the server is not the leader
func (s *RaftSurfstore) CheckIsLeader() error {
	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()

	if !s.isLeader {
		return ERR_NOT_LEADER
	}
	return nil
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}
	if err := s.CheckIsLeader(); err != nil {
		return nil, err
	}
	fileInfoMap, err := s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	return fileInfoMap, err
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}
	if err := s.CheckIsLeader(); err != nil {
		return nil, err
	}
	blockStoreMap, err := s.metaStore.GetBlockStoreMap(ctx, hashes)
	return blockStoreMap, err
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}
	if err := s.CheckIsLeader(); err != nil {
		return nil, err
	}
	blockStoreAddrs, err := s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	return blockStoreAddrs, err
}

// block until majority recovered, only the leader calls this function
func (s *RaftSurfstore) waitRecovery() {
	for {
		if err := s.CheckIsCrashed(); err != nil {
			return
		}
		if err := s.CheckIsLeader(); err != nil {
			return
		}

		succCount := make(chan int, 1)
		// s.broadcastEmpty(succCount)
		s.broadcast(succCount)
		count := <-succCount

		if count > len(s.peers)/2 {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}
	if err := s.CheckIsLeader(); err != nil {
		return nil, err
	}

	// TODO: update file
	// append entry to log
	log.Println("append:", filemeta)
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})

	// wait recovery
	s.waitRecovery()

	// should check again
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}
	if err := s.CheckIsLeader(); err != nil {
		return nil, err
	}

	// send entry to all followers in parallel
	succCount := make(chan int)
	go s.broadcast(succCount)

	// FIXME: keep trying indefinitely even after responding (rely on SendHeartBeat)

	// commit the entry once replicated to the majority of followers
	totalSucc := <-succCount
	if totalSucc > len(s.peers)/2 {
		s.commitIndex++
		// FIXME: notify the followers to commit (rely on SendHeartBeat)
		// apply to the state machine
		s.lastApplied++
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, fmt.Errorf("unable to replicate to the majority of followers")
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// fmt.Println(input)
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}

	// TODO: append entries
	// reply false if term < currentTerm
	if input.Term < s.term {
		return nil, fmt.Errorf("request term %d is less than current term %d", input.Term, s.term)
	}
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = input.Term
	}

	if input.Entries != nil {
		log.Println(input.Entries)
		s.log = input.Entries
	}

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = input.LeaderCommit
	}

	for s.lastApplied < s.commitIndex {
		s.metaStore.UpdateFile(ctx, s.log[s.lastApplied+1].FileMetaData)
		s.lastApplied++
	}

	output := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: int64(len(s.log) - 1),
	}
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}

	// set leader
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term++

	// initialize leader data structures
	// for idx := range s.peers {
	// 	s.nextIndex[idx] = len(s.log)
	// 	s.matchIndex[idx] = 0
	// }

	// TODO: update state
	return &Success{Flag: true}, nil
}

// broadcast with nil log
func (s *RaftSurfstore) broadcastEmpty(succCount chan int) {
	// prevLogIndex := int64(s.nextIndex[idx] - 1)
	// prevLogTerm := s.log[prevLogIndex].Term
	input := &AppendEntryInput{
		Term: s.term,
		// TODO: put the right values
		// PrevLogIndex: prevLogIndex,
		// PrevLogTerm:  prevLogTerm,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      nil,
		LeaderCommit: -1,
	}
	s._broadcast(input, succCount)
}

func (s *RaftSurfstore) broadcast(succCount chan int) {
	// prevLogIndex := int64(s.nextIndex[idx] - 1)
	// prevLogTerm := s.log[prevLogIndex].Term
	input := &AppendEntryInput{
		Term: s.term,
		// TODO: put the right values
		// PrevLogIndex: prevLogIndex,
		// PrevLogTerm:  prevLogTerm,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	s._broadcast(input, succCount)
}

// send AppendEntries to all the followers in parallel, reply the number of successes
func (s *RaftSurfstore) _broadcast(input *AppendEntryInput, succCount chan int) {
	replies := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.send(addr, input, replies)
	}

	totalSucc := 1
	totalReplies := 1
	for {
		succ := <-replies
		totalReplies++
		if succ {
			totalSucc++
		}

		if totalReplies == len(s.peers) {
			break
		}
	}

	succCount <- totalSucc
}

// send one AppendEntries request to the specified addr with the given input
func (s *RaftSurfstore) send(addr string, input *AppendEntryInput, response chan bool) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("failed connecting to %s: %v\n", addr, err)
		response <- false
		return
	}

	c := NewRaftSurfstoreClient(conn)
	// FIXME: use the correct timeout for AppendEntries
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = c.AppendEntries(ctx, input); err != nil {
		// log.Printf("append entries failed on %s: %v\n", addr, err)
		response <- false
		return
	}

	response <- true
}

// send a round of heartbeats to all other nodes, append entries if any
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.CheckIsCrashed(); err != nil {
		return nil, err
	}
	// if not the leader, do not send heartbeat
	if err := s.CheckIsLeader(); err != nil {
		return &Success{Flag: false}, nil
	}

	succCount := make(chan int, 1)
	s.broadcast(succCount)
	count := <-succCount

	if count > len(s.peers)/2 {
		return &Success{Flag: true}, nil
	} else {
		return &Success{Flag: false}, nil
	}
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
