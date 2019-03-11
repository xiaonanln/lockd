package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"

	"github.com/xiaonanln/lockd/raft"
)

type DemoRaftInstance struct {
	ctx      context.Context
	id       int
	recvChan chan raft.RecvRPCMessage
	Raft     *raft.Raft

	healthy       xnsyncutil.AtomicPointer
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

func getInstance(id int) *DemoRaftInstance {
	instancesLock.RLock()
	defer instancesLock.RUnlock()
	return instances[id]
}

func getAllInstances() (inss map[int]*DemoRaftInstance) {
	instancesLock.RLock()
	defer instancesLock.RUnlock()
	inss = map[int]*DemoRaftInstance{}
	for id, ins := range instances {
		inss[id] = ins
	}
	return
}

func (ins *DemoRaftInstance) String() string {
	return fmt.Sprintf("DemoRaftInstance<%d>", ins.id)
}
func (ins *DemoRaftInstance) ID() int {
	return ins.id
}

func (ins *DemoRaftInstance) Crash() {
	// clear all messages in recvChan
clearloop:
	for {
		select {
		case <-ins.recvChan:
			continue clearloop
		default:
			break clearloop
		}
	}

	ins.sumAllNumbers = 0
	ins.Raft.Shutdown()

	ins.Raft = raft.NewRaft(ins.ctx, INSTANCE_NUM, ins, ins)
}

func (ins *DemoRaftInstance) Recv() <-chan raft.RecvRPCMessage {
	return ins.recvChan
}

func (ins *DemoRaftInstance) Send(insID int, msg raft.RPCMessage) {
	h := ins.GetHealthy()

	if !h.CanSend() {
		return
	}

	if rand.Float32() < h.MessageDropRate {
		// can send message, but message is dropped
		return
	}

	dstInstance := getInstance(insID)
	dstH := dstInstance.GetHealthy()
	assert.Truef(demoLogger, dstInstance != ins, "%s Send to %d,%s, msg=%#v", ins, insID, dstInstance, msg)
	if !dstH.CanRecv() {
		return
	}

	totalDelay := time.Millisecond + h.SendDelay + dstH.RecvDelay
	time.AfterFunc(totalDelay, func() {
		dstInstance.recvChan <- raft.RecvRPCMessage{ins.ID(), msg.Copy()}
	})
}

// Broadcast sends message to all other instances
func (ins *DemoRaftInstance) Broadcast(msg raft.RPCMessage) {
	allInstances := getAllInstances()
	for id, dst := range allInstances {
		if dst != ins {
			ins.Send(id, msg)
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

func (ins *DemoRaftInstance) SetHealthy(healthy *InstanceHealthy) {
	ins.healthy.Store(unsafe.Pointer(healthy))
	if healthy.Crash {
		demoLogger.Warnf("%s CRASHED!", ins)
		ins.Crash()
	} else if healthy.NetworkDown {
		demoLogger.Warnf("%s NETWORK DOWN!", ins)
	} else {
		demoLogger.Warnf("%s IS ALIVE!", ins)
	}
}

func (ins *DemoRaftInstance) GetHealthy() *InstanceHealthy {
	return (*InstanceHealthy)(ins.healthy.Load())
}
