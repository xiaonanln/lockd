package raft

type RPCMessage interface {
	GetTerm() Term
	Copy() RPCMessage
}

type AppendEntriesMessage struct {
	//GetTerm leader’s GetTerm
	Term Term
	//leaderId so follower can redirect clients
	leaderId int
	//prevLogIndex Index of LogList entry immediately preceding new ones
	prevLogIndex LogIndex
	//prevLogTerm GetTerm of prevLogIndex entry
	prevLogTerm Term
	//entries[] LogList entries to store (empty for heartbeat; may send more than one for efficiency)
	entries []*Log
	//leaderCommit leader’s CommitIndex
	leaderCommit LogIndex
}

func (m *AppendEntriesMessage) GetTerm() Term {
	return m.Term
}

func (msg *AppendEntriesMessage) Copy() RPCMessage {
	copy := *msg
	// copy entries from old to new
	copy.entries = append([]*Log{}, msg.entries...)
	return &copy
}

type AppendEntriesACKMessage struct {
	Term    Term
	success bool
	// last LogList Index of the AppendEntries message when success, equals to prevLogIndex if entries is empty
	lastLogIndex LogIndex
}

func (msg *AppendEntriesACKMessage) Copy() RPCMessage {
	copy := *msg
	return &copy
}

func (m *AppendEntriesACKMessage) GetTerm() Term {
	return m.Term
}

type RequestVoteMessage struct {
	// GetTerm candidate’s GetTerm
	Term Term
	// candidateId candidate requesting vote
	candidateId int
	// lastLogIndex Index of candidate’s last LogList entry
	lastLogIndex LogIndex
	//lastLogTerm GetTerm of candidate’s last LogList entry
	lastLogTerm Term
}

func (m *RequestVoteMessage) GetTerm() Term {
	return m.Term
}
func (msg *RequestVoteMessage) Copy() RPCMessage {
	copy := *msg
	return &copy
}

type RequestVoteACKMessage struct {
	Term        Term
	voteGranted bool
}

func (msg *RequestVoteACKMessage) Copy() RPCMessage {
	copy := *msg
	return &copy
}

func (m *RequestVoteACKMessage) GetTerm() Term {
	return m.Term
}

//Invoked by leader to send chunks of a snapshot to a follower
type InstallSnapshotRPCMessage struct {
	Term      Term     // leader's term
	leaderID  int      // so follower can redirect clients
	lastIndex LogIndex // the snapshot replaces all entries up through and including this index
	lastTerm  Term     // term of lastIndex
	//lastConfig string   // latest cluster configuration as of lastIndex (include only with first chunk)
	//offset     int64    // byte offset where chunk is positioned in the snapshot file
	data []byte // raw bytes of snapshot chunk starting at offset
	//done bool   //true if this is the last chunk
}

func (msg *InstallSnapshotRPCMessage) GetTerm() Term {
	return msg.Term
}

func (msg *InstallSnapshotRPCMessage) Copy() RPCMessage {
	copy := *msg
	return &copy
}

type InstallSnapshotACKMessage struct {
	Term Term
}

func (msg *InstallSnapshotACKMessage) GetTerm() Term {
	return msg.Term
}

func (msg *InstallSnapshotACKMessage) Copy() RPCMessage {
	copy := *msg
	return &copy
}
