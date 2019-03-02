package demo

import (
	"context"
	"fmt"
	"github.com/xiaonanln/lockd/raft"
	"time"

	"sync"

	"math/rand"
	"strconv"
)

type DemoRaftInstance struct {
	ctx          context.Context
	id           int
	recvChan     chan raft.RecvRPCMessage
	logInputChan chan raft.LogData
}

var (
	instancesLock sync.RWMutex
	instances     = map[int]*DemoRaftInstance{}
)

func init() {
	go inputLogRoutine()
}

func inputLogRoutine() {
	for {
		time.Sleep(time.Millisecond * 100)
		// choose a random instance to input

		instancesLock.RLock()

		if len(instances) > 0 {
			inslist := make([]*DemoRaftInstance, 0, len(instances))
			for _, ins := range instances {
				inslist = append(inslist, ins)
			}
			randomData := []byte(strconv.Itoa(rand.Intn(1000)))
			inslist[rand.Intn(len(inslist))].logInputChan <- randomData
			//log.Printf("INPUT >>> %s", string(randomData))
		}

		instancesLock.RUnlock()

	}
}

func NewDemoRaftInstance(ctx context.Context, id int) *DemoRaftInstance {
	ins := &DemoRaftInstance{
		ctx:          ctx,
		id:           id,
		recvChan:     make(chan raft.RecvRPCMessage, 1000),
		logInputChan: make(chan raft.LogData, 10),
	}

	instancesLock.Lock()
	instances[id] = ins
	instancesLock.Unlock()
	return ins
}

func (ins *DemoRaftInstance) String() string {
	return fmt.Sprintf("RaftInstance<%d>", ins.id)
}
func (ins *DemoRaftInstance) ID() int {
	return ins.id
}

func (ins *DemoRaftInstance) Recv() <-chan raft.RecvRPCMessage {
	return ins.recvChan
}

func (ins *DemoRaftInstance) Send(insID int, msg raft.RPCMessage) {
	instancesLock.RLock()
	instances[insID].recvChan <- raft.RecvRPCMessage{ins.ID(), msg}
	instancesLock.RUnlock()
}

// Broadcast sends message to all other instances
func (ins *DemoRaftInstance) Broadcast(msg raft.RPCMessage) {
	//log.Printf("%s BROADCAST: %+v", ins, msg)
	instancesLock.RLock()
	defer instancesLock.RUnlock()
	for _, other := range instances {
		if other == ins {
			continue
		}

		other.recvChan <- raft.RecvRPCMessage{ins.ID(), msg}
	}
}

func (ins *DemoRaftInstance) InputLog() <-chan raft.LogData {
	return ins.logInputChan
}

func (ins *DemoRaftInstance) Input(data []byte) {
	ins.logInputChan <- data
}
