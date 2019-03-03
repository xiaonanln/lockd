package raft

import (
	"fmt"
	"strings"
)

type Term = uint64

const InvalidTerm = 0

type LogIndex = uint64

const InvalidLogIndex = 0

type LogData []byte

type WorkMode int

func (m WorkMode) String() string {
	return [3]string{"follower", "candidate", "leader"}[m]
}

const (
	Follower WorkMode = iota
	Candidate
	Leader
)

type RecvRPCMessage struct {
	SenderID int
	Message  RPCMessage
}

func (d LogData) String() string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, b := range d {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%02x", b))
	}
	sb.WriteByte(']')
	return sb.String()
}

type NetworkDevice interface {
	ID() int
	Recv() <-chan RecvRPCMessage
	Send(instanceID int, msg RPCMessage)
	Broadcast(msg RPCMessage)
}

type StateMachine interface {
	Apply(log *Log)
}

type RPCMessage interface {
	GetTerm() Term
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

type AppendEntriesACKMessage struct {
	Term    Term
	success bool
	// last LogList Index of the AppendEntries message when success, equals to prevLogIndex if entries is empty
	lastLogIndex LogIndex
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

type RequestVoteACKMessage struct {
	Term        Term
	voteGranted bool
}

func (m *RequestVoteACKMessage) GetTerm() Term {
	return m.Term
}

type Log struct {
	Term  Term
	Index LogIndex
	Data  LogData
}

type Logger interface {
	Errorf(format string, args ...interface{})
}
