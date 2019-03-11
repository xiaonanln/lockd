package raft

import (
	"fmt"
	"io"
	"strings"
)

type Term = uint64

const InvalidTerm = Term(0)

type LogIndex = uint64

const InvalidLogIndex = LogIndex(0)

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

type Transport interface {
	ID() int
	Recv() <-chan RecvRPCMessage
	Send(instanceID int, msg RPCMessage)
	Broadcast(msg RPCMessage)
}

type StateMachine interface {
	ApplyLog(log []byte)
	Snapshot(w io.Writer) error
	InstallSnapshot(r io.Reader) error
}

type Log struct {
	Term  Term
	Index LogIndex
	Data  LogData
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

type Snapshot struct {
	Data      []byte
	LastTerm  Term
	LastIndex LogIndex
}

func (ss *Snapshot) String() string {
	if ss != nil {
		return fmt.Sprintf("Snapshot<%d#%d,size=%d>", ss.LastTerm, ss.LastIndex, len(ss.Data))
	} else {
		return fmt.Sprintf("NoSnapshot<0#0>")
	}
}
