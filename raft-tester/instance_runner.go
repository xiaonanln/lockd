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
	peers                 []raft.TransportID
	instancesLock         sync.RWMutex
	instances             map[raft.TransportID]*DemoRaftInstance
	lastLeaderCommitIndex raft.LogIndex
	currentPeriod         *TimePeriod
}

func newInstanceRunner(ctx context.Context, quorum int) *InstanceRunner {
	r := &InstanceRunner{
		ctx:       ctx,
		quorum:    quorum,
		instances: map[raft.TransportID]*DemoRaftInstance{},

		lastLeaderCommitIndex: raft.InvalidLogIndex,
	}

	for i := 0; i < quorum; i++ {
		id := raft.TransportID(strconv.Itoa(i))
		r.peers = append(r.peers, id)
		ins := r.newDemoRaftInstance(id, quorum)
		r.instances[raft.TransportID(strconv.Itoa(i))] = ins
	}

	return r
}

func (runner *InstanceRunner) newDemoRaftInstance(id raft.TransportID, quorum int) *DemoRaftInstance {
	ins := &DemoRaftInstance{
		runner:   runner,
		ctx:      runner.ctx,
		id:       id,
		quorum:   quorum,
		recvChan: make(chan raft.RecvRPCMessage, 1000),
	}
	peers := []raft.TransportID{}
	for i := 0; i < ins.quorum; i++ {
		peers = append(peers, raft.TransportID(strconv.Itoa(i)))
	}
	ins.Raft = raft.NewRaft(ins.ctx, ins.runner.quorum, ins, ins, peers)
	ins.healthy.Store(unsafe.Pointer(&InstanceHealthyPerfect)) // all instances area perfectly healthy when started

	runner.instancesLock.Lock()
	runner.instances[id] = ins
	runner.instancesLock.Unlock()
	return ins
}

func (runner *InstanceRunner) getInstance(id raft.TransportID) *DemoRaftInstance {
	runner.instancesLock.RLock()
	defer runner.instancesLock.RUnlock()
	return runner.instances[id]
}

func (runner *InstanceRunner) getAllInstances() (inss map[raft.TransportID]*DemoRaftInstance) {
	runner.instancesLock.RLock()
	defer runner.instancesLock.RUnlock()
	inss = map[raft.TransportID]*DemoRaftInstance{}
	for id, ins := range runner.instances {
		inss[id] = ins
	}
	return
}

func (runner *InstanceRunner) Run() {
	verifyCounter := 0
	inputCounter := 0

	runner.currentPeriod = runner.newTimePeriod()
	runner.applyTimePeriod()
	for {
		randInsID := raft.TransportID(strconv.Itoa(rand.Intn(runner.quorum)))
		inputCounter = inputCounter + 1
		inputData := strconv.Itoa(inputCounter)
		r := runner.instances[randInsID].Raft
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
			runner.currentPeriod = runner.newTimePeriod()
			runner.applyTimePeriod()
		}
	}
}

func (runner *InstanceRunner) applyTimePeriod() {
	for _, peerID := range runner.peers {
		ins := runner.instances[peerID]
		ins.SetHealthy(runner.currentPeriod.instanceHealthy[peerID])
	}
}

func (runner *InstanceRunner) verifyCorrectness() {
	// verify the correctness of Raft algorithm
	// lock all Raft instances before
	lock := func() {
		for _, peerID := range runner.peers {
			runner.instances[peerID].Raft.Lock()
		}
	}

	unlock := func() {
		for _, peerID := range runner.peers {
			runner.instances[peerID].Raft.Unlock()
		}
	}

	lock()
	defer unlock()

	leader := runner.findLeader()
	followers := []*raft.Raft{}
	for i := 0; i < runner.quorum; i++ {
		id := raft.TransportID(strconv.Itoa(i))
		r := runner.instances[id].Raft
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
		assert.GreaterOrEqual(assertLogger, leader.LogList.LastIndex(), runner.lastLeaderCommitIndex, "leader=%s, lastLeaderCommitIndex=%v, followers=%v", leader, runner.lastLeaderCommitIndex, followers)
	}
	runner.lastLeaderCommitIndex = leaderCommitIndex

	isAllAppliedLogIndexSame := true    // determine if all rafts has same LastAppliedIndex
	minLogIndex := raft.InvalidLogIndex // to find the first log logIndex that <= leader.CommitIndex and exists on all raft instances
	// other raft instances should not have larger commit logIndex
	for _, id := range runner.peers {
		r := runner.instances[id].Raft
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

		for _, id := range runner.peers {
			r := runner.instances[id].Raft
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
		for i := 0; i < runner.quorum-1; i++ {
			assert.True(assertLogger, runner.instances[raft.TransportID(strconv.Itoa(i))].StateMachineEquals(runner.instances[raft.TransportID(strconv.Itoa(i+1))]))
		}
		demoLogger.Infof("CONGRATULATIONS! ALL REPLICATED STATE MACHINES ARE CONSISTENT.")
	}

	log.Printf("Verify correctness ok: leader's commit logIndex = %v, check entries range: %v ~ %v", leaderCommitIndex, minLogIndex, leaderCommitIndex)
}

func (runner *InstanceRunner) findLeader() *raft.Raft {
	leaders := []raft.TransportID{}
	for _, id := range runner.peers {
		if runner.instances[id].Raft.Mode() == raft.Leader {
			leaders = append(leaders, id)
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

		if voteCount >= (runner.quorum/2)+1 {
			// this is the real leader
			return leaderR
		} else {
			return nil
		}
	}
}

func (runner *InstanceRunner) newTimePeriod() *TimePeriod {
	oldTimePeriod := runner.currentPeriod

	tp := &TimePeriod{
		startTime:       time.Now(),
		duration:        PERIOD_DURATION,
		instanceHealthy: map[raft.TransportID]*InstanceHealthy{},
	}

	// calculate how many instances were crashed in the previous period
	oldCrashNum := 0
	if oldTimePeriod != nil {
		for _, h := range oldTimePeriod.instanceHealthy {
			if h.Crash || h.NetworkDown {
				oldCrashNum += 1
			}
		}
	}

	maxBrokenNum := (runner.quorum - 1) / 2
	assert.LessOrEqual(assertLogger, oldCrashNum, maxBrokenNum)
	maxBrokenNum -= oldCrashNum

	brokenNum := rand.Intn(maxBrokenNum + 1)
	for i, id := range runner.peers {
		brokenProb := float64(brokenNum) / float64((runner.quorum - i))
		if rand.Float64() < brokenProb {
			// this instance should be broken
			tp.instanceHealthy[id] = newBrokenInstanceHealthy()
			brokenNum -= 1
		} else {
			// this instance should be healthy
			tp.instanceHealthy[id] = newNormalInstanceHealthy()
		}
	}
	return tp
}
