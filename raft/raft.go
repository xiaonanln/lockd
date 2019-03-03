package raft

// Read raft paper at https://raft.github.io/raft.pdf

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	electionTimeout                = time.Millisecond * 100
	maxStartElectionDelay          = electionTimeout / 2
	leaderAppendEntriesRPCInterval = time.Millisecond * 10
)

type Raft struct {
	sync.Mutex // for locking

	ins         NetworkDevice
	ctx         context.Context
	instanceNum int
	mode        WorkMode

	// raft states
	currentTerm Term
	votedFor    int
	LogList     []*Log
	// Index of highest LogList entry known to be committed (initialized to 0, increases monotonically)
	CommitIndex LogIndex
	// Index of highest LogList entry applied to state machine (initialized to 0, increases monotonically)
	LastAppliedIndex LogIndex

	// all state fields
	resetElectionTimeoutTime time.Time

	// follower mode fields

	// candidate mode fields
	startElectionTime time.Time
	electionStarted   bool
	voteGrantedCount  int

	// leader mode fields
	lastAppendEntriesRPCTime time.Time
	// for each server, Index of the next LogList entry to send to that server (initialized to leader last LogList Index + 1)
	nextIndex []LogIndex
	// for each server, Index of highest LogList entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []LogIndex

	Logger   Logger
	logInput chan []LogData
}

func NewRaft(ctx context.Context, instanceNum int, ins NetworkDevice) *Raft {
	if instanceNum <= 0 {
		log.Fatalf("instanceNum should be larger than 0")
	}

	if ins.ID() >= instanceNum {
		log.Fatalf("instance ID should be smaller than %d", instanceNum)
	}

	raft := &Raft{
		ins:                      ins,
		ctx:                      ctx,
		instanceNum:              instanceNum,
		mode:                     Follower,
		resetElectionTimeoutTime: time.Now(),
		// init raft states
		currentTerm:      0,
		votedFor:         -1,
		LogList:          nil,
		CommitIndex:      0,
		LastAppliedIndex: 0,
		Logger:           defaultSugaredLogger,
	}

	go raft.routine()

	return raft
}

func (r *Raft) ID() int {
	return r.ins.ID()
}

func (r *Raft) String() string {
	return fmt.Sprintf("Raft<%d|%d#%d|C%d|A%d>", r.ins.ID(), r.currentTerm, r.lastLogIndex(), r.CommitIndex, r.LastAppliedIndex)
}

func (r *Raft) Mode() WorkMode {
	return r.mode
}

func (r *Raft) Input(data LogData) (term Term, index LogIndex) {
	assert.Truef(r.Logger, r.mode == Leader, "not leader")

	term = r.currentTerm
	index = r.lastLogIndex() + 1
	newlog := &Log{
		Term:  term,
		Index: index,
		Data:  data,
	}

	r.LogList = append(r.LogList, newlog)
	//log.Printf("%s LOG %d#%d DATA=%s", r, r.lastLogTerm(), r.lastLogIndex(), string(newlog.Data))
	return
}

func (r *Raft) routine() {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	log.Printf("%s start running ...", r)

forloop:
	for {
		select {
		case <-ticker.C:
			// If CommitIndex > LastAppliedIndex: increment LastAppliedIndex, apply
			// LogList[LastAppliedIndex] to state machine (§5.3)
			r.Lock() // TODO: lock-free mode

			switch r.mode {
			case Leader:
				r.leaderTick()
			case Candidate:
				r.candidateTick()
			case Follower:
				r.followerTick()
			default:
				log.Fatalf("invalid mode: %d", r.mode)
			}

			r.Unlock()

		case recvMsg := <-r.ins.Recv():
			//LogList.Printf("%s received msg: %+v", r, msg)
			r.Lock()
			r.handleMsg(recvMsg.SenderID, recvMsg.Message)
			r.Unlock()
		case <-r.ctx.Done():
			log.Printf("%s stopped.", r)
			break forloop
		}
	}
}

func (r *Raft) handleMsg(senderID int, _msg RPCMessage) {
	//All Servers:
	//?If RPC request or response contains GetTerm T > currentTerm:
	//set currentTerm = T, convert to follower (?.1)

	if r.currentTerm < _msg.GetTerm() {
		r.enterFollowerMode(_msg.GetTerm())
	}

	switch msg := _msg.(type) {
	case *AppendEntriesACKMessage:
		r.handleAppendEntriesACK(senderID, msg)
	case *AppendEntriesMessage:
		r.handleAppendEntries(msg)
	case *RequestVoteMessage:
		r.handleRequestVote(msg)
	case *RequestVoteACKMessage:
		r.handleRequestVoteACKMessage(msg)
	default:
		log.Fatalf("unexpected message type: %T", _msg)
	}
}

func (r *Raft) handleRequestVote(msg *RequestVoteMessage) {
	grantVote := r._handleRequestVote(msg)
	log.Printf("%s grant vote %+v: %v", r, msg, grantVote)
	// send grant vote ACK
	// Results:
	//GetTerm currentTerm, for candidate to update itself
	//voteGranted true means candidate received vote
	ackMsg := &RequestVoteACKMessage{
		Term:        r.currentTerm,
		voteGranted: grantVote,
	}
	r.ins.Send(msg.candidateId, ackMsg)
}

func (r *Raft) _handleRequestVote(msg *RequestVoteMessage) bool {
	//1. Reply false if GetTerm < currentTerm (§5.1)
	//2. If votedFor is null or candidateId, and candidate’s LogList is at least as up-to-date as receiver’s LogList, grant vote (§5.2, §5.4)

	if msg.Term < r.currentTerm {
		return false
	}

	grantVote := (r.votedFor == -1 || r.votedFor == msg.candidateId) && r.isLogUpToDate(msg.lastLogTerm, msg.lastLogIndex)
	if grantVote {
		r.votedFor = msg.candidateId
	}
	return grantVote
}

func (r *Raft) handleRequestVoteACKMessage(msg *RequestVoteACKMessage) {
	if r.mode != Candidate || msg.Term != r.currentTerm {
		// if not in candidate mode anymore, just ignore this packet
		return
	}

	if msg.voteGranted {
		r.voteGrantedCount += 1
		if r.voteGrantedCount >= (r.instanceNum/2)+1 {
			// become leader
			r.enterLeaderMode()
		}
	}
}

func (r *Raft) handleAppendEntriesACK(senderID int, msg *AppendEntriesACKMessage) {
	if r.mode != Leader {
		// not leader anymore..
		return
	}

	// assert msg.GetTerm <= r.currentTerm
	if msg.success {
		r.nextIndex[senderID] = msg.lastLogIndex + 1
		if msg.lastLogIndex > r.matchIndex[senderID] {
			r.matchIndex[senderID] = msg.lastLogIndex
		}
	} else {
		// AppendEntries fail
		// decrement nextIndex, but nextIndex should be at least 1
		if r.nextIndex[senderID] > 1 {
			r.nextIndex[senderID] -= 1
		}
	}
}

func (r *Raft) handleAppendEntries(msg *AppendEntriesMessage) {
	success, lastLogIndex := r.handleAppendEntriesImpl(msg)

	r.ins.Send(msg.leaderId, &AppendEntriesACKMessage{
		Term:         r.currentTerm,
		success:      success,
		lastLogIndex: lastLogIndex,
	})

}

func (r *Raft) handleAppendEntriesImpl(msg *AppendEntriesMessage) (bool, LogIndex) {
	//1. Reply false if GetTerm < currentTerm (§5.1)
	// If this is the leader, then msg.GetTerm < r.currentTerm should always be satisfied, because Raft assures that msg.GetTerm != r.currentTer
	if msg.Term < r.currentTerm {
		return false, 0
	}

	if r.mode == Leader {
		log.Fatalf("should not be leader")
	}

	r.resetElectionTimeout()
	//2. Reply false if LogList doesn’t contain an entry at prevLogIndex whose GetTerm matches prevLogTerm (§5.3)
	containsPrevLog, prevIdx := r.containsLog(msg.prevLogTerm, msg.prevLogIndex)
	if !containsPrevLog {
		return false, 0
	}

	replaceIdxStart := prevIdx + 1

	for i, entry := range msg.entries {
		if replaceIdxStart+i < len(r.LogList) {
			//3. If an existing entry conflicts with a new one (same Index but different terms), delete the existing entry and all that follow it (§5.3)
			replaceLog := r.LogList[replaceIdxStart+i]
			if replaceLog.Index != entry.Index {
				log.Fatalf("LogList Index Mismatch: %d & %d", replaceLog.Index, entry.Index)
			}
			if replaceLog.Term == entry.Term {
				// normal case
			} else {
				replaceLog.Term, replaceLog.Data = entry.Term, entry.Data
				r.LogList = r.LogList[0 : replaceIdxStart+i+1]
			}
		} else {
			//4. Append any new entries not already in the LogList
			r.LogList = append(r.LogList, entry)
		}
	}

	//var lastLogTerm Term
	var lastLogIndex LogIndex
	if len(msg.entries) > 0 {
		//lastLogTerm = msg.entries[len(msg.entries)-1].Term
		lastLogIndex = msg.entries[len(msg.entries)-1].Index
	} else {
		//lastLogTerm = msg.prevLogTerm
		lastLogIndex = msg.prevLogIndex
	}
	//5. If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, Index of last new entry)
	//log.Printf("%s LOG %d.%d", r, lastLogTerm, lastLogIndex)
	if msg.leaderCommit > r.CommitIndex {
		commitIndex := msg.leaderCommit
		if commitIndex > lastLogIndex {
			commitIndex = lastLogIndex
		}
		if commitIndex < r.CommitIndex {
			log.Fatalf("New Commit Index Is %d, But Current Commit Index Is %d", commitIndex, r.CommitIndex)
		}
		if commitIndex > r.CommitIndex {
			r.CommitIndex = commitIndex
			//log.Printf("%s COMMITS %d", r, r.CommitIndex)
		}
	}

	return true, lastLogIndex
}

// isLogUpToDate determines if the LogList of specified GetTerm and Index is at least as up-to-date as r.LogList
func (r *Raft) isLogUpToDate(term Term, logIndex LogIndex) bool {
	myterm := r.lastLogTerm()
	if term < myterm {
		return false
	} else if term > myterm {
		return true
	}

	return logIndex >= r.lastLogIndex()
}

func (r *Raft) followerTick() {
	now := time.Now()
	if now.Sub(r.resetElectionTimeoutTime) > electionTimeout {
		r.enterCandidateMode()
	}
}

func (r *Raft) candidateTick() {
	now := time.Now()

	if now.Sub(r.resetElectionTimeoutTime) > electionTimeout {
		log.Printf("%s: election timeout in candidate mode, restarting election ...", r)
		r.prepareElection()
		return
	}

	if !r.electionStarted {
		if now.After(r.startElectionTime) {
			r.startElection()
		}
	}

}

func (r *Raft) leaderTick() {
	r.tryCommitLogs()

	now := time.Now()
	if now.Sub(r.lastAppendEntriesRPCTime) >= leaderAppendEntriesRPCInterval {
		// time to broadcast AppendEntriesRPC
		//LogList.Printf("%s: Broadcast AppendEntries ...", r)
		r.lastAppendEntriesRPCTime = now
		r.broadcastAppendEntries()
	}
}

func (r *Raft) resetElectionTimeout() {
	r.resetElectionTimeoutTime = time.Now()
}

func (r *Raft) prepareElection() {
	r.assureInMode(Candidate)

	log.Printf("%s prepare election ...", r)
	r.startElectionTime = time.Now().Add(time.Duration(rand.Int63n(int64(maxStartElectionDelay))))
	r.electionStarted = false
	r.resetElectionTimeout()
	log.Printf("%s set start election time = %s", r, r.startElectionTime)
}

func (r *Raft) startElection() {
	r.assureInMode(Candidate)

	if r.electionStarted {
		log.Panicf("election already started")
	}
	//On conversion to candidate, start election:
	//?Increment currentTerm
	//?Vote for self
	//?Reset election timer
	//?Send RequestVote RPCs to all other servers
	log.Printf("%s start election ...", r)
	r.newTerm(r.currentTerm + 1)
	r.electionStarted = true
	r.votedFor = r.ID()    // vote for self
	r.voteGrantedCount = 1 // vote for self in the beginning of election
	r.sendRequestVote()
}

func (r *Raft) sendRequestVote() {
	//Arguments:
	//GetTerm candidate’s GetTerm
	//candidateId candidate requesting vote
	//lastLogIndex Index of candidate’s last LogList entry (§5.4)
	//lastLogTerm GetTerm of candidate’s last LogList entry (§5.4)
	msg := &RequestVoteMessage{
		Term:         r.currentTerm,
		candidateId:  r.ID(),
		lastLogIndex: r.lastLogIndex(),
		lastLogTerm:  r.lastLogTerm(),
	}
	r.ins.Broadcast(msg)
}

func (r *Raft) assureInMode(mode WorkMode) {
	if r.mode != mode {
		log.Fatalf("%s should in %s mode, but in %s mode", r, mode, r.mode)
	}
}

// enter follower mode with new GetTerm
func (r *Raft) enterFollowerMode(term Term) {
	log.Printf("%s change mode: %s ==> %s, new GetTerm = %d", r, r.mode, Follower, term)
	r.newTerm(term)
	r.mode = Follower
}

func (r *Raft) newTerm(term Term) {
	if r.currentTerm >= term {
		log.Fatalf("current GetTerm is %d, can not enter follower mode with GetTerm %d", r.currentTerm, term)
	}
	r.currentTerm = term
	r.votedFor = -1
	r.voteGrantedCount = 0
}

func (r *Raft) enterCandidateMode() {
	if r.mode != Follower {
		log.Fatalf("only follower can convert to candidate, but current mode is %s", r.mode)
	}

	log.Printf("%s change mode: %s ==> %s", r, r.mode, Candidate)
	r.mode = Candidate
	r.prepareElection()
}

func (r *Raft) enterLeaderMode() {
	if r.mode != Candidate {
		log.Fatalf("only candidate can convert to leader, but current mode is %s", r.mode)
	}

	log.Printf("%s change mode: %s ==> %s", r, r.mode, Leader)
	r.mode = Leader
	log.Printf("NEW LEADER ELECTED: %d, term=%v, granted=%d, quorum=%d !!!", r.ID(), r.currentTerm, r.voteGrantedCount, r.instanceNum)
	r.lastAppendEntriesRPCTime = time.Time{}
	r.nextIndex = make([]LogIndex, r.instanceNum)
	for i := range r.nextIndex {
		r.nextIndex[i] = r.lastLogIndex() + 1
	}
	r.matchIndex = make([]LogIndex, r.instanceNum)
}

func (r *Raft) lastLogIndex() LogIndex {
	if len(r.LogList) > 0 {
		return r.LogList[len(r.LogList)-1].Index
	} else {
		return 0
	}
}

func (r *Raft) lastLogTerm() Term {
	if len(r.LogList) > 0 {
		return r.LogList[len(r.LogList)-1].Term
	} else {
		return 0
	}
}
func (r *Raft) broadcastAppendEntries() {
	for insID := 0; insID < r.instanceNum; insID++ {
		if insID == r.ID() {
			continue
		}

		logIndex := r.nextIndex[insID] // next Index for the instance to receive
		var entries []*Log
		nextlogidx := r.locateLog(logIndex)
		var prevLogTerm Term
		var prevLogIndex LogIndex
		if nextlogidx > 0 {
			prevLogTerm = r.LogList[nextlogidx-1].Term
			prevLogIndex = r.LogList[nextlogidx-1].Index
		}

		for i := nextlogidx; i < len(r.LogList); i++ {
			entries = append(entries, r.LogList[i])
		}

		//if len(entries) > 0 {
		//	log.Printf("%s APPEND %d LOG %d ~ %d", r, insID, nextlogidx, len(r.LogList)-1)
		//}

		msg := &AppendEntriesMessage{
			Term:         r.currentTerm,
			leaderId:     r.ID(),
			prevLogTerm:  prevLogTerm,
			prevLogIndex: prevLogIndex,
			leaderCommit: r.CommitIndex,
			entries:      entries,
		}

		r.ins.Send(insID, msg)
	}
}

func (r *Raft) locateLog(index LogIndex) int {
	r.validateLog()
	return int(index) - 1
}

func (r *Raft) containsLog(term Term, index LogIndex) (bool, int) {
	idx := r.locateLog(index)
	if idx == -1 {
		return true, -1
	} else if idx >= len(r.LogList) {
		return false, idx
	}

	_log := r.LogList[idx]
	if _log.Index != index {
		log.Fatalf("Should Equal")
	}
	return _log.Term == term, idx
}

func (r *Raft) validateLog() {
	prevLogIndex := LogIndex(0)
	for _, _log := range r.LogList {
		if _log.Index != prevLogIndex+1 {
			log.Fatalf("Invalid LogList Index: %d, Should be %d", _log.Index, prevLogIndex+1)
		}
		prevLogIndex = _log.Index
	}
}
func (r *Raft) tryCommitLogs() {
	//If there exists an N such that N > CommitIndex, a majority
	//of matchIndex[i] ≥ N, and LogList[N].GetTerm == currentTerm:
	//set CommitIndex = N (§5.3, §5.4).
	for idx := len(r.LogList) - 1; idx >= 0; idx-- {
		_log := r.LogList[idx]
		if _log.Index <= r.CommitIndex || _log.Term != r.currentTerm {
			// nothing to commit
			break
		}
		// _log.Index > r.CommitIndex && _log.GetTerm == r.currentTerm, check if we can commit this _log
		commitCount := 1
		for id, matchIndx := range r.matchIndex {
			if id != r.ID() && matchIndx >= _log.Index {
				commitCount += 1
			}
		}
		if commitCount >= r.instanceNum/2+1 {
			// commited
			r.CommitIndex = _log.Index
			//log.Printf("%s COMMITS %d", r, r.CommitIndex)
			break
		}
	}
}
