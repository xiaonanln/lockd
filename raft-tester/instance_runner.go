package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/xiaonanln/lockd/raft"
)

type InstanceRunner struct {
	ctx                   context.Context
	quorum                int
	instancesLock         sync.RWMutex
	instances             map[int]*DemoRaftInstance
	lastLeaderCommitIndex raft.LogIndex
	currentPeriod         *TimePeriod
}

func newInstanceRunner(ctx context.Context, quorum int) *InstanceRunner {
	r := &InstanceRunner{
		ctx:       ctx,
		quorum:    quorum,
		instances: map[int]*DemoRaftInstance{},

		lastLeaderCommitIndex: raft.InvalidLogIndex,
	}

	for i := 0; i < quorum; i++ {
		ins := r.newDemoRaftInstance(i)
		r.instances[i] = ins
	}

	return r
}

func (runner *InstanceRunner) newDemoRaftInstance(id int) *DemoRaftInstance {
	ins := &DemoRaftInstance{
		runner:   runner,
		ctx:      runner.ctx,
		id:       id,
		recvChan: make(chan raft.RecvRPCMessage, 1000),
	}
	ins.Raft = raft.NewRaft(ins.ctx, INSTANCE_NUM, ins, ins)
	ins.healthy.Store(unsafe.Pointer(&InstanceHealthyPerfect)) // all instances area perfectly healthy when started

	runner.instancesLock.Lock()
	runner.instances[id] = ins
	runner.instancesLock.Unlock()
	return ins
}

func (runner *InstanceRunner) getInstance(id int) *DemoRaftInstance {
	runner.instancesLock.RLock()
	defer runner.instancesLock.RUnlock()
	return runner.instances[id]
}

func (runner *InstanceRunner) getAllInstances() (inss map[int]*DemoRaftInstance) {
	runner.instancesLock.RLock()
	defer runner.instancesLock.RUnlock()
	inss = map[int]*DemoRaftInstance{}
	for id, ins := range runner.instances {
		inss[id] = ins
	}
	return
}

func (runner *InstanceRunner) Run() {
	verifyCounter := 0
	inputCounter := 0

	runner.currentPeriod = newTimePeriod()
	runner.applyTimePeriod()
	for {
		randInsIdx := rand.Intn(INSTANCE_NUM)
		inputCounter = inputCounter + 1
		inputData := strconv.Itoa(inputCounter)
		r := runner.instances[randInsIdx].Raft
		r.Lock()
		if r.Mode() == raft.Leader {
			r.Input([]byte(inputData))
		}
		r.Unlock()

		time.Sleep(time.Microsecond * 1)
		verifyCounter += 1
		if verifyCounter%1000 == 0 {
			runner.verifyCorrectness()
		}

		if runner.currentPeriod.isTimeout() {
			runner.currentPeriod = newTimePeriod()
			runner.applyTimePeriod()
		}
	}
}

func (runner *InstanceRunner) applyTimePeriod() {
	for i := 0; i < INSTANCE_NUM; i++ {
		ins := runner.instances[i]
		ins.SetHealthy(runner.currentPeriod.instanceHealthy[i])
	}
}

func (runner *InstanceRunner) verifyCorrectness() {
	// verify the correctness of Raft algorithm
	// lock all Raft instances before
	lock := func() {
		for i := 0; i < INSTANCE_NUM; i++ {
			runner.instances[i].Raft.Lock()
		}
	}

	unlock := func() {
		for i := 0; i < INSTANCE_NUM; i++ {
			runner.instances[i].Raft.Unlock()
		}
	}

	lock()
	defer unlock()

	leader := runner.findLeader()
	followers := []*raft.Raft{}
	for i := 0; i < INSTANCE_NUM; i++ {
		r := runner.instances[i].Raft
		r.VerifyCorrectness()
		if r != leader {
			followers = append(followers, r)
		}
	}

	if leader == nil {
		log.Printf("leader not found: followers: %v", followers)
	} else {
		log.Printf("found leader: %v, followers: %v", leader, followers)
	}

	// 监查每个Raft实例，确定所有Commit部分都完全相同。
	if leader == nil {
		return
	}

	leaderCommitIndex := leader.CommitIndex
	if leaderCommitIndex < runner.lastLeaderCommitIndex {
		// the current leader's CommitIndex is less than before (previous leader's CommitIndex)
		// which is a normal case when new leader is elected
		// In this case, the new leader has less CommitIndex, but it must contains logs to previous leader's CommitIndex
		assert.GreaterOrEqual(assertLogger, leader.LogList.LastIndex(), runner.lastLeaderCommitIndex)
	}
	runner.lastLeaderCommitIndex = leaderCommitIndex

	isAllAppliedLogIndexSame := true    // determine if all rafts has same LastAppliedIndex
	minLogIndex := raft.InvalidLogIndex // to find the first log logIndex that <= leader.CommitIndex and exists on all raft instances
	// other raft instances should not have larger commit logIndex
	for i := 0; i < INSTANCE_NUM; i++ {
		r := runner.instances[i].Raft
		//assert.LessOrEqual(demoLogger, r.CommitIndex, leaderCommitIndex) // not necessary so
		if minLogIndex < r.LogList.SnapshotLastIndex()+1 {
			minLogIndex = r.LogList.SnapshotLastIndex() + 1
		}
		if r.LastAppliedIndex != leader.LastAppliedIndex {
			isAllAppliedLogIndexSame = false
		}
	}

	// make sure all commited logs are exactly same
	for logIndex := minLogIndex; logIndex <= leaderCommitIndex; logIndex++ {
		leaderLog := leader.LogList.GetLog(logIndex)
		assert.Equal(assertLogger, logIndex, leaderLog.Index)
		logTerm := leaderLog.Term
		logData := leaderLog.Data

		for i := 0; i < INSTANCE_NUM; i++ {
			r := runner.instances[i].Raft
			if r != leader && logIndex <= r.CommitIndex {
				followerLog := r.LogList.GetLog(logIndex)
				assert.Equal(assertLogger, logIndex, followerLog.Index)
				assert.Equal(assertLogger, logTerm, followerLog.Term)
				assert.Equal(assertLogger, logData, followerLog.Data)
			}
		}
	}

	// make sure all state machines are consistent
	if isAllAppliedLogIndexSame {
		for i := 0; i < INSTANCE_NUM-1; i++ {
			assert.True(assertLogger, runner.instances[i].StateMachineEquals(runner.instances[i+1]))
		}
		demoLogger.Infof("CONGRATULATIONS! ALL REPLICATED STATE MACHINES ARE CONSISTENT.")
	}

	log.Printf("Verify correctness ok: leader's commit logIndex = %v, check entries range: %v ~ %v", leaderCommitIndex, minLogIndex, leaderCommitIndex)
}

func (runner *InstanceRunner) findLeader() *raft.Raft {
	leaders := []int{}
	for i := 0; i < INSTANCE_NUM; i++ {
		if runner.instances[i].Raft.Mode() == raft.Leader {
			leaders = append(leaders, i)
		}
	}

	if len(leaders) == 1 {
		return runner.instances[leaders[0]].Raft
	} else if len(leaders) == 0 {
		return nil
	} else {
		// found multiple leaders, use the one with highest commited index as the real leader
		bestLeader := leaders[0]
		leaderR := runner.instances[bestLeader].Raft
		for i := 1; i < len(leaders); i++ {
			if runner.instances[leaders[i]].Raft.CommitIndex > runner.instances[bestLeader].Raft.CommitIndex {
				bestLeader = leaders[i]
				leaderR = runner.instances[bestLeader].Raft
			}
		}

		// make sure this leader is voted by majority of members
		voteCount := 0
		for i := 0; i < len(leaders); i++ {
			r := runner.instances[leaders[i]].Raft
			if r.CurrentTerm == leaderR.CurrentTerm && r.VotedFor == bestLeader {
				voteCount += 1
			}
		}

		if voteCount >= (INSTANCE_NUM/2)+1 {
			// this is the real leader
			return leaderR
		} else {
			return nil
		}
	}
}
