package raft

// Read raft paper at https://raft.github.io/raft.pdf

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

const (
	electionTimeout                = time.Millisecond * 100
	maxStartElectionDelay          = electionTimeout / 2
	leaderAppendEntriesRPCInterval = time.Millisecond * 10
)

type LogList struct {
	Snapshot *Snapshot
	Logs     []*Log
}

func (ll *LogList) String() string {
	return fmt.Sprintf("LogList[%s->%d#%d]", ll.Snapshot, ll.LastTerm(), ll.LastIndex())
}

func (ll *LogList) LastTerm() Term {
	if len(ll.Logs) > 0 {
		return ll.Logs[len(ll.Logs)-1].Term
	} else if ll.Snapshot != nil {
		return ll.Snapshot.LastTerm
	} else {
		return InvalidTerm
	}
}

func (ll *LogList) LastIndex() LogIndex {
	if len(ll.Logs) > 0 {
		return ll.Logs[len(ll.Logs)-1].Index
	} else if ll.Snapshot != nil {
		return ll.Snapshot.LastIndex
	} else {
		return InvalidLogIndex
	}
}

func (ll *LogList) SnapshotLastTerm() Term {
	if ll.Snapshot != nil {
		return ll.Snapshot.LastTerm
	} else {
		return InvalidTerm
	}
}

func (ll *LogList) SnapshotLastIndex() LogIndex {
	if ll.Snapshot != nil {
		return ll.Snapshot.LastIndex
	} else {
		return InvalidLogIndex
	}
}

func (ll *LogList) FindLogBefore(logIndex LogIndex) (ok bool, prevTerm Term, prevLogIndex LogIndex) {
	idx := ll.LogIndexToIndex(logIndex)
	if idx > 0 {
		//defaultSugaredLogger.Infof("idx=%v, ll.Logs=%v", idx, len(ll.Logs))
		prevLog := ll.Logs[idx-1]
		prevTerm, prevLogIndex = prevLog.Term, prevLog.Index
		ok = true
	} else if idx == 0 {
		// this is the first log, so the previous one is already applied
		prevTerm, prevLogIndex = ll.SnapshotLastTerm(), ll.SnapshotLastIndex()
		ok = true
	} else {
		// idx < 0 ? can not handle this case now, todo: handle this case
		// log is already snapchated, we have to send snapchat to the client
		//assert.Failf(defaultSugaredLogger, "FindLogBefore", "can not find prev term & index for log index %v, PrevLogIndex=%v", logIndex, ll.PrevLogIndex)
		prevTerm, prevLogIndex = InvalidTerm, InvalidLogIndex
		ok = false
	}
	return
}

func (ll *LogList) GetEntries(startLogIndex LogIndex) []*Log {
	startIdx := ll.LogIndexToIndex(startLogIndex)
	return ll.Logs[startIdx:]
}

func (ll *LogList) Append(data []byte, term Term) *Log {
	index := ll.LastIndex() + 1
	newlog := &Log{
		Term:  term,
		Index: index,
		Data:  data,
	}

	ll.Logs = append(ll.Logs, newlog)
	return newlog
}

func (ll *LogList) AppendEntries(prevTerm Term, prevIndex LogIndex, entries []*Log) bool {
	foundPrevLog, prevIdx := ll.LocateLog(prevTerm, prevIndex)
	if !foundPrevLog {
		return false
	}

	replaceIdx := prevIdx + 1
	readIdx := 0
	if replaceIdx < 0 {
		// previous log is applied already, so we advance read index by the number of logs in entries that are already applied
		readIdx += (-replaceIdx)
	}

	for ; readIdx < len(entries); readIdx++ {
		entry := entries[readIdx]

		if replaceIdx < len(ll.Logs) {
			//3. If an existing entry conflicts with a new one (same Index but different terms), delete the existing entry and all that follow it (§5.3)
			replaceLog := ll.Logs[replaceIdx]
			assert.Equal(defaultSugaredLogger, entry.Index, replaceLog.Index)

			if replaceLog.Term == entry.Term {
				// normal case
			} else {
				replaceLog.Term, replaceLog.Data = entry.Term, entry.Data
				ll.Logs = ll.Logs[0 : replaceIdx+1]
			}
		} else {
			//4. Append any new entries not already in the LogList
			ll.Logs = append(ll.Logs, entry)
		}

		replaceIdx += 1

	}
	return true
}

// LocateLog find the index for the log index
func (ll *LogList) LogIndexToIndex(index LogIndex) int {
	snapshotLastIndex := ll.SnapshotLastIndex()
	if index >= snapshotLastIndex {
		return int(index-snapshotLastIndex) - 1
	} else {
		return -int(snapshotLastIndex-index) - 1
	}
}

func (ll *LogList) LocateLog(term Term, index LogIndex) (bool, int) {
	idx := ll.LogIndexToIndex(index)
	if idx < 0 {
		// the log is already in snapshot, term must match because all snapshot are committed
		return true, idx
	}

	if idx >= len(ll.Logs) {
		return false, -1
	}

	log := ll.Logs[idx]
	assert.Equal(defaultSugaredLogger, log.Index, index)
	if log.Term != term {
		return false, -1 // log found with different term
	}

	return true, idx
}

//func (ll *LogList) RemoveApplied(num int) {
//	removedEntries := ll.Logs[:num]
//	ll.Logs = ll.Logs[num:]
//	lastRemovedLog := removedEntries[len(removedEntries)-1]
//	ll.PrevLogTerm = lastRemovedLog.Term
//	ll.PrevLogIndex = lastRemovedLog.Index
//}

func (ll *LogList) GetLog(logIndex LogIndex) *Log {
	idx := ll.LogIndexToIndex(logIndex)
	//defaultSugaredLogger.Infof("try go get log index=%v, idx=%v, but logs = %v", logIndex, idx, ll.Logs)
	return ll.Logs[idx]
}

func (ll *LogList) SetSnapshot(snapshot *Snapshot) bool {
	assert.True(defaultSugaredLogger, snapshot.LastTerm > InvalidTerm)
	assert.True(defaultSugaredLogger, snapshot.LastIndex > InvalidLogIndex)

	if ll.Snapshot != nil && ll.Snapshot.LastIndex >= snapshot.LastIndex {
		// no need to install it because my snapshot is even newer
		//defaultSugaredLogger.Infof("set snapshot ignored: LastIndex=%d, existing snapshot.LastIndex=%d", snapshot.LastIndex, ll.Snapshot.LastIndex)
		return false
	}

	// ll.Snapshot == nil || ll.Snapshot.LastIndex > snapshot.LastIndex
	localSnapshotLastIndex := ll.SnapshotLastIndex()
	ll.Snapshot = snapshot

	// remove all logs that is already in loglist with Index < snapshot.LastIndex
	discardLogNum := int(snapshot.LastIndex - localSnapshotLastIndex)
	if discardLogNum > len(ll.Logs) {
		discardLogNum = len(ll.Logs)
	}

	if discardLogNum > 0 {
		ll.Logs = ll.Logs[discardLogNum:]
	}

	//defaultSugaredLogger.Infof("set snapshot succeed: snapshot.LastIndex=%d, last log = %d#%d", ll.SnapshotLastIndex(), ll.LastTerm(), ll.LastIndex())
	return true
}

type Raft struct {
	sync.Mutex // for locking

	transport   Transport
	ss          StateMachine
	ctx         context.Context
	instanceNum int
	mode        WorkMode

	// raft states
	currentTerm Term
	votedFor    int
	LogList     LogList
	// Index of highest LogList entry known to be committed (initialized to 0, increases monotonically)
	CommitIndex LogIndex
	// Index of highest LogList entry applied to state machine (initialized to 0, increases monotonically)
	LastAppliedIndex LogIndex
	LastAppliedTerm  Term

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
	nextIndex           []LogIndex
	nextIndexSearchStep []LogIndex
	// for each server, Index of highest LogList entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []LogIndex

	Logger   Logger
	logInput chan []LogData
}

func NewRaft(ctx context.Context, instanceNum int, transport Transport, ss StateMachine) *Raft {
	if instanceNum <= 0 {
		log.Fatalf("instanceNum should be larger than 0")
	}

	if transport.ID() >= instanceNum {
		log.Fatalf("instance ID should be smaller than %d", instanceNum)
	}

	raft := &Raft{
		transport:                transport,
		ss:                       ss,
		ctx:                      ctx,
		instanceNum:              instanceNum,
		mode:                     Follower,
		resetElectionTimeoutTime: time.Now(),
		// init raft states
		currentTerm:      InvalidTerm,
		votedFor:         -1,
		LogList:          LogList{},
		CommitIndex:      InvalidLogIndex,
		LastAppliedIndex: 0,
		Logger:           defaultSugaredLogger,
	}

	go raft.routine()

	return raft
}

func (r *Raft) ID() int {
	return r.transport.ID()
}

func (r *Raft) String() string {
	return fmt.Sprintf("Raft<%d|%v|%d|%s|C%d|A%d>", r.transport.ID(), r.mode, r.currentTerm, &r.LogList, r.CommitIndex, r.LastAppliedIndex)
}

func (r *Raft) Mode() WorkMode {
	return r.mode
}

func (r *Raft) Input(data LogData) (term Term, index LogIndex) {
	assert.Truef(r.Logger, r.mode == Leader, "not leader")
	term = r.currentTerm
	newlog := r.LogList.Append(data, term)
	index = newlog.Index
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

		case recvMsg := <-r.transport.Recv():
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
	//?If RPC request or response contains Term T > currentTerm:
	//set currentTerm = T, convert to follower (?.1)

	msgTerm := _msg.GetTerm()
	if r.currentTerm < msgTerm {
		r.enterFollowerMode(msgTerm)
	}

	switch msg := _msg.(type) {
	case *AppendEntriesACKMessage:
		r.handleAppendEntriesACK(senderID, msg)
	case *AppendEntriesMessage:
		r.verifyAppendEntriesSound(msg.entries)
		r.handleAppendEntries(msg)
	case *RequestVoteMessage:
		r.handleRequestVote(msg)
	case *RequestVoteACKMessage:
		r.handleRequestVoteACKMessage(msg)
	case *InstallSnapshotMessage:
		r.handleInstallSnapshotMessage(msg)
	case *InstallSnapshotACKMessage:
		r.handleInstallSnapshotACKMessage(senderID, msg)
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
	r.transport.Send(msg.candidateId, ackMsg)
}

func (r *Raft) _handleRequestVote(msg *RequestVoteMessage) bool {
	//1. Reply false if Term < currentTerm (§5.1)
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

func (r *Raft) handleInstallSnapshotACKMessage(senderID int, msg *InstallSnapshotACKMessage) {
	// nothing todo here
	if r.mode != Leader {
		return
	}

	if msg.Term < r.currentTerm {
		return
	}

	if msg.success {
		r.nextIndex[senderID] = msg.lastLogIndex + 1
		if msg.lastLogIndex > r.matchIndex[senderID] {
			r.matchIndex[senderID] = msg.lastLogIndex
		}
		r.nextIndexSearchStep[senderID] = 1 // reset search step to 1 when AppendEntriesRPC succeed
	} else {
		// Install Snapshot Failed, can do nothing here.
	}
}

func (r *Raft) handleAppendEntriesACK(senderID int, msg *AppendEntriesACKMessage) {
	if r.mode != Leader {
		// not leader anymore..
		return
	}

	if msg.Term < r.currentTerm {
		// Ack of previous term ... Meaning this leader was leader of msg.Term and now becomes the leader of current term
		// Should ignore this RPC? This is a very very rare case and I think it is OK to ignore.
		return
	}

	//r.Logger.Infof("%s.handleAppendEntriesACK: %v success=%v", r, senderID, msg.success)
	// assert msg.GetTerm <= r.currentTerm
	if msg.success {
		r.nextIndex[senderID] = msg.lastLogIndex + 1
		if msg.lastLogIndex > r.matchIndex[senderID] {
			r.matchIndex[senderID] = msg.lastLogIndex
		}
		r.nextIndexSearchStep[senderID] = 1 // reset search step to 1 when AppendEntriesRPC succeed

		//r.Logger.Infof("%s.handleAppendEntriesACK success: nextIndex[%v] <== %v", r, senderID, r.nextIndex[senderID])
	} else {
		// AppendEntries fail
		// decrement nextIndex, but nextIndex should be at least 1
		step := r.nextIndexSearchStep[senderID]
		if r.nextIndex[senderID]-step > 0 {
			r.nextIndex[senderID] -= step
		} else {
			r.nextIndex[senderID] = 1
		}
		r.Logger.Infof("%s.handleAppendEntriesACK failed: nextIndex[%v] <== %v, step = %v", r, senderID, r.nextIndex[senderID], step)
		r.nextIndexSearchStep[senderID] *= 2
	}
}

func (r *Raft) handleAppendEntries(msg *AppendEntriesMessage) {
	success, lastLogIndex := r.handleAppendEntriesImpl(msg)

	r.transport.Send(msg.leaderID, &AppendEntriesACKMessage{
		Term:         r.currentTerm,
		success:      success,
		lastLogIndex: lastLogIndex,
	})

}

func (r *Raft) handleAppendEntriesImpl(msg *AppendEntriesMessage) (bool, LogIndex) {
	//1. Reply false if Term < currentTerm (§5.1)
	// If this is the leader, then msg.GetTerm < r.currentTerm should always be satisfied, because Raft assures that msg.GetTerm != r.currentTer
	if msg.Term < r.currentTerm {
		return false, 0
	}

	assert.True(r.Logger, r.mode != Leader)

	if r.mode == Candidate {
		// candidate should convert to follower on AppendEntries RPC
		// mst.Term should be equal to r.currentTerm for this moment
		assert.Equal(r.Logger, msg.Term, r.currentTerm)
		r.enterFollowerMode(msg.Term)
	}

	r.resetElectionTimeout()
	//2. Reply false if LogList doesn’t contain an entry at prevLogIndex whose Term matches prevLogTerm (§5.3)
	//r.Logger.Infof("%s.AppendEntries: %+v", r, msg)
	appendOk := r.LogList.AppendEntries(msg.prevLogTerm, msg.prevLogIndex, msg.entries)
	//r.Logger.Infof("%s.handleAppendEntriesImpl: Ok=%v", r, appendOk)
	if !appendOk {
		return false, 0
	}

	//var lastLogTerm Term
	var lastLogIndex LogIndex
	if len(msg.entries) > 0 {
		//lastLogTerm = msg.entries[len(msg.entries)-1].Term
		assert.NotNil(r.Logger, msg.entries[len(msg.entries)-1])
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

func (r *Raft) handleInstallSnapshotMessage(msg *InstallSnapshotMessage) {
	// todo: install snapshot
	success, lastSnapshotLogIndex := r.handleInstallSnapshotRPCMessageImpl(msg)

	// reply to leader no matter install success or failed
	//r.Logger.Errorf("Sending InstallSnapshotACKMessage to Leader %v", msg.leaderID)
	r.transport.Send(msg.leaderID, &InstallSnapshotACKMessage{
		Term:         r.currentTerm,
		success:      success,
		lastLogIndex: lastSnapshotLogIndex,
	})
}
func (r *Raft) handleInstallSnapshotRPCMessageImpl(msg *InstallSnapshotMessage) (bool, LogIndex) {
	if msg.Term < r.currentTerm {
		return false, InvalidLogIndex
	}

	assert.True(r.Logger, r.mode != Leader)

	if r.mode == Candidate {
		// candidate should convert to follower on InstallSnapshot RPC (I believe so because InstallSnapshot is another form of AppendEntries)
		// mst.Term should be equal to r.currentTerm for this moment
		assert.Equal(r.Logger, msg.Term, r.currentTerm)
		r.enterFollowerMode(msg.Term)
	}

	r.resetElectionTimeout()

	// if the snapshot's LastIndex is smaller than our snapshot's, just ignore this snapshot
	recvSnapshot := msg.Snapshot

	// the received snapshot is newer than our's
	// install the snapshot to the LogList and discard log up to snapshot's last term#index
	if !r.LogList.SetSnapshot(recvSnapshot) {
		//r.Logger.Infof("%s ignored snapshot %s, because local snapshot is #%d", r, recvSnapshot, r.LogList.SnapshotLastIndex())
		return true, r.LogList.SnapshotLastIndex()
	}

	// Snapshot from leader implies that leader's commitIndex >= snapshot.LastIndex
	leaderCommit := recvSnapshot.LastIndex
	if leaderCommit > r.CommitIndex {
		r.CommitIndex = leaderCommit
		//log.Printf("%s COMMITS %d", r, r.CommitIndex)
	}

	// install snapshot to the machine is as same as apply logs to the last term#index of the snapshot
	ssrd := bytes.NewBuffer(recvSnapshot.Data)
	err := r.ss.InstallSnapshot(ssrd)
	assert.Nil(r.Logger, err)
	r.LastAppliedTerm = recvSnapshot.LastTerm
	r.LastAppliedIndex = recvSnapshot.LastIndex

	//r.Logger.Infof("%s installed received snapshot %s, local log is %s", r, recvSnapshot, &r.LogList)

	return true, r.LogList.SnapshotLastIndex()
}

// isLogUpToDate determines if the LogList of specified Term and Index is at least as up-to-date as r.LogList
func (r *Raft) isLogUpToDate(term Term, logIndex LogIndex) bool {
	myterm := r.LogList.LastTerm()
	if term < myterm {
		return false
	} else if term > myterm {
		return true
	}

	return logIndex >= r.LogList.LastIndex()
}

func (r *Raft) followerTick() {
	r.tryApplyCommitedLogs()
	r.tryCompactLog()

	now := time.Now()
	if now.Sub(r.resetElectionTimeoutTime) > electionTimeout {
		r.enterCandidateMode()
	}
}

func (r *Raft) candidateTick() {
	r.tryApplyCommitedLogs()
	r.tryCompactLog()

	now := time.Now()

	if now.Sub(r.resetElectionTimeoutTime) > electionTimeout {
		//log.Printf("%s: election timeout in candidate mode, restarting election ...", r)
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
	r.tryApplyCommitedLogs()
	r.tryCompactLog()

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

	//log.Printf("%s prepare election in term %d...", r, r.currentTerm)
	r.startElectionTime = time.Now().Add(time.Duration(rand.Int63n(int64(maxStartElectionDelay))))
	r.electionStarted = false
	r.resetElectionTimeout()
	//log.Printf("%s set start election time = %s", r, r.startElectionTime)
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
	//log.Printf("%s start election ...", r)
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
	//lastLogTerm Term of candidate’s last LogList entry (§5.4)
	msg := &RequestVoteMessage{
		Term:         r.currentTerm,
		candidateId:  r.ID(),
		lastLogIndex: r.LogList.LastIndex(),
		lastLogTerm:  r.LogList.LastTerm(),
	}
	r.transport.Broadcast(msg)
}

func (r *Raft) assureInMode(mode WorkMode) {
	if r.mode != mode {
		log.Fatalf("%s should in %s mode, but in %s mode", r, mode, r.mode)
	}
}

// enter follower mode with new GetTerm
func (r *Raft) enterFollowerMode(term Term) {
	log.Printf("%s change mode: %s ==> %s, new Term = %d", r, r.mode, Follower, term)
	assert.GreaterOrEqual(r.Logger, term, r.currentTerm)

	r.mode = Follower
	if term > r.currentTerm {
		r.newTerm(term)
	}
}

func (r *Raft) newTerm(term Term) {
	if r.currentTerm >= term {
		log.Fatalf("current Term is %d, can not enter follower mode with Term %d", r.currentTerm, term)
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
	r.nextIndexSearchStep = make([]LogIndex, r.instanceNum)
	for i := range r.nextIndex {
		r.nextIndex[i] = r.LogList.LastIndex() + 1
		r.nextIndexSearchStep[i] = 1
	}

	r.matchIndex = make([]LogIndex, r.instanceNum)
}

func (r *Raft) broadcastAppendEntries() {
	// TODO: use broadcast if all followers have same `nextIndex`
	for insID := 0; insID < r.instanceNum; insID++ {
		if insID == r.ID() {
			continue
		}

		nextLogIndex := r.nextIndex[insID]   // next Index for the instance to receive
		matchLogIndex := r.matchIndex[insID] // matched log index for this instance
		ok, prevLogTerm, prevLogIndex := r.LogList.FindLogBefore(nextLogIndex)
		if ok {
			var entries []*Log
			//r.Logger.Infof("%s => %s nextLogIndex = %v, matchLogIndex=%v", r, insID, nextLogIndex, matchLogIndex)
			if nextLogIndex > matchLogIndex+1 {
				// next log index is too big, send empty entries for discovering nextLogIndex
				entries = []*Log{}
			} else {
				entries = r.LogList.GetEntries(nextLogIndex)
				r.verifyAppendEntriesSound(entries)
			}

			msg := &AppendEntriesMessage{
				Term:         r.currentTerm,
				leaderID:     r.ID(),
				prevLogTerm:  prevLogTerm,
				prevLogIndex: prevLogIndex,
				leaderCommit: r.CommitIndex,
				entries:      entries,
			}
			r.transport.Send(insID, msg)
		} else {
			// this follower is too far behind to append entries, we need install snapshot to it
			assert.True(r.Logger, r.LogList.Snapshot != nil)
			snapshot := r.LogList.Snapshot
			msg := &InstallSnapshotMessage{
				Term:     r.currentTerm,
				leaderID: r.ID(),
				Snapshot: snapshot,
			}
			r.transport.Send(insID, msg)
		}
	}
}

func (r *Raft) tryCommitLogs() {
	//If there exists an N such that N > CommitIndex, a majority
	//of matchIndex[i] ≥ N, and LogList[N].GetTerm == currentTerm:
	//set CommitIndex = N (§5.3, §5.4).
	for idx := len(r.LogList.Logs) - 1; idx >= 0; idx-- {
		_log := r.LogList.Logs[idx]
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

func (r *Raft) tryApplyCommitedLogs() {
	assert.LessOrEqual(r.Logger, uint64(r.LastAppliedIndex), uint64(r.CommitIndex))
	startApplyLogIndex := r.LastAppliedIndex + 1
	stopApplyLogIndex := r.CommitIndex
	if startApplyLogIndex >= stopApplyLogIndex {
		return
	}

	for applyLogIndex := r.LastAppliedIndex + 1; applyLogIndex <= r.CommitIndex; applyLogIndex++ {
		// apply the log
		applyIdx := r.LogList.LogIndexToIndex(applyLogIndex)
		// TODO: What if follower received a snapshot and installed to the log list, but failed to install it to the statemachine.
		// TODO: In this case, the follower's LastAppliedIndex is smaller than the LastIndex of the snapshot, leading to applyIdx < 0
		assert.GreaterOrEqual(r.Logger, applyIdx, 0)         // assert applyIdx >= 0 because applyIdx is not applied yet, so it is not removed yet
		assert.Less(r.Logger, applyIdx, len(r.LogList.Logs)) // assert applyIdx is valid index, because applyLogIndex <= r.CommitIndex
		applyLog := r.LogList.Logs[applyIdx]
		//r.Logger.Infof("%s APPLY %d#%d", r, applyLog.Term, applyLog.Index)
		assert.Equal(r.Logger, applyLogIndex, applyLog.Index)

		r.ss.ApplyLog(applyLog.Data)
		r.LastAppliedIndex = applyLogIndex
		r.LastAppliedTerm = applyLog.Term
	}
}

func (r *Raft) tryCompactLog() error {
	if r.LastAppliedIndex >= r.LogList.SnapshotLastIndex()+100 {
		buf := bytes.NewBuffer([]byte{})
		//TODO: take snapshot in another goroutine
		err := r.ss.Snapshot(buf)
		if err != nil {
			defaultSugaredLogger.Warnf("Snapshot failed: %v", err)
			return errors.Wrapf(err, "snapshot failed: LastAppliedIndex=%v", r.LastAppliedIndex)
		}

		if r.LogList.SetSnapshot(&Snapshot{buf.Bytes(), r.LastAppliedTerm, r.LastAppliedIndex}) {
			//r.Logger.Infof("%s compact log: set snapshot to %d#%d", r, r.LastAppliedTerm, r.LastAppliedIndex)
		}
	}
	return nil
}

// VerifyCorrectness make sure the Raft status is correct
func (r *Raft) VerifyCorrectness() {

}

func (r *Raft) verifyAppendEntriesSound(entries []*Log) {
	//assert.NotNil(r.Logger, entries)
	for i := 0; i < len(entries); i++ {
		assert.NotNil(r.Logger, entries[i])
	}
}
