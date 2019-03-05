package demo

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/xiaonanln/lockd/raft"
)

type DemoRaftInstance struct {
	ctx      context.Context
	id       int
	recvChan chan raft.RecvRPCMessage
	Raft     *raft.Raft
	IsBroken bool

	sumAllNumbers int
}

var (
	instancesLock sync.RWMutex
	instances     = map[int]*DemoRaftInstance{}
)

func NewDemoRaftInstance(ctx context.Context, id int) *DemoRaftInstance {
	ins := &DemoRaftInstance{
		ctx:      ctx,
		id:       id,
		recvChan: make(chan raft.RecvRPCMessage, 1000),
	}

	instancesLock.Lock()
	instances[id] = ins
	instancesLock.Unlock()
	return ins
}

func (ins *DemoRaftInstance) String() string {
	return fmt.Sprintf("NetworkDevice<%d>", ins.id)
}
func (ins *DemoRaftInstance) ID() int {
	return ins.id
}

func (ins *DemoRaftInstance) Recv() <-chan raft.RecvRPCMessage {
	return ins.recvChan
}

func (ins *DemoRaftInstance) Send(insID int, msg raft.RPCMessage) {
	if ins.IsBroken {
		return
	}

	instancesLock.RLock()
	instances[insID].recvChan <- raft.RecvRPCMessage{ins.ID(), msg.Copy()}
	instancesLock.RUnlock()
}

// Broadcast sends message to all other instances
func (ins *DemoRaftInstance) Broadcast(msg raft.RPCMessage) {
	if ins.IsBroken {
		return
	}

	//log.Printf("%s BROADCAST: %+v", ins, msg)
	instancesLock.RLock()
	defer instancesLock.RUnlock()
	for _, other := range instances {
		if other == ins {
			continue
		}

		other.recvChan <- raft.RecvRPCMessage{ins.ID(), msg.Copy()}
	}
}

func (ins *DemoRaftInstance) ApplyLog(data []byte) {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err)
	}

	ins.sumAllNumbers += n
}

func (ins *DemoRaftInstance) StateMachineEquals(other *DemoRaftInstance) bool {
	return ins.sumAllNumbers == other.sumAllNumbers
}

func (ins *DemoRaftInstance) SetBroken(broken bool) {
	ins.IsBroken = broken
}
