package demo

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"

	"github.com/xiaonanln/lockd/raft"
)

type DemoRaftInstance struct {
	ctx      context.Context
	id       int
	recvChan chan raft.RecvRPCMessage
	Raft     *raft.Raft
	IsBroken xnsyncutil.AtomicBool

	sumAllNumbers int64
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
	if ins.IsBroken.Load() {
		return
	}

	instancesLock.RLock()
	if !instances[insID].IsBroken.Load() {
		instances[insID].recvChan <- raft.RecvRPCMessage{ins.ID(), msg.Copy()}
	}
	instancesLock.RUnlock()
}

// Broadcast sends message to all other instances
func (ins *DemoRaftInstance) Broadcast(msg raft.RPCMessage) {
	if ins.IsBroken.Load() {
		return
	}

	//log.Printf("%s BROADCAST: %+v", ins, msg)
	instancesLock.RLock()
	defer instancesLock.RUnlock()
	for _, other := range instances {
		if other == ins {
			continue
		}
		if !other.IsBroken.Load() {
			other.recvChan <- raft.RecvRPCMessage{ins.ID(), msg.Copy()}
		}
	}
}

func (ins *DemoRaftInstance) ApplyLog(data []byte) {
	n, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err)
	}

	ins.sumAllNumbers += int64(n)
}

func (ins *DemoRaftInstance) Snapshot(w io.Writer) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(ins.sumAllNumbers))
	_, err := w.Write(b[:])
	return err
}

func (ins *DemoRaftInstance) InstallSnapshot(r io.Reader) error {
	var b [8]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return err
	}

	v := binary.LittleEndian.Uint64(b[:])
	ins.sumAllNumbers = int64(v)
	return nil
}

func (ins *DemoRaftInstance) StateMachineEquals(other *DemoRaftInstance) bool {
	return ins.sumAllNumbers == other.sumAllNumbers
}

func (ins *DemoRaftInstance) SetBroken(broken bool) {
	ins.IsBroken.Store(broken)
}
