package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"

	"github.com/xiaonanln/lockd/raft"
)

type DemoRaftInstance struct {
	runner   *InstanceRunner
	ctx      context.Context
	id       int
	recvChan chan raft.RecvRPCMessage
	Raft     *raft.Raft

	healthy       xnsyncutil.AtomicPointer
	sumAllNumbers int64
}

func (ins *DemoRaftInstance) String() string {
	return fmt.Sprintf("DemoRaftInstance<%d>", ins.id)
}
func (ins *DemoRaftInstance) ID() int {
	return ins.id
}

func (ins *DemoRaftInstance) Recover() {
	ins.Raft.Shutdown() // duplicate Shutdown, but should be fine
	ins.sumAllNumbers = 0
	ins.Raft = raft.NewRaft(ins.ctx, INSTANCE_NUM, ins, ins)
}

func (ins *DemoRaftInstance) Crash() {
	// clear all messages in recvChan
	ins.Raft.Lock()
clearloop:
	for {
		select {
		case <-ins.recvChan:
			continue clearloop
		default:
			break clearloop
		}
	}

	ins.Raft.Shutdown()
	ins.Raft.Unlock()
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

	dstInstance := ins.runner.getInstance(insID)
	dstH := dstInstance.GetHealthy()
	assert.Truef(assertLogger, dstInstance != ins, "%s Send to %d,%s, msg=%#v", ins, insID, dstInstance, msg)
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
	allInstances := ins.runner.getAllInstances()
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
	previousHealthy := ins.GetHealthy()

	ins.healthy.Store(unsafe.Pointer(healthy))

	if !previousHealthy.Crash && healthy.Crash {
		ins.Crash()
	} else if previousHealthy.Crash && !healthy.Crash {
		ins.Recover()
	}

	if healthy.Crash {
		demoLogger.Warnf("%s CRASHED!", ins)
	} else if healthy.NetworkDown {
		demoLogger.Warnf("%s NETWORK DOWN!", ins)
	} else {
		demoLogger.Warnf("%s IS ALIVE!", ins)
	}
}

func (ins *DemoRaftInstance) GetHealthy() *InstanceHealthy {
	return (*InstanceHealthy)(ins.healthy.Load())
}
