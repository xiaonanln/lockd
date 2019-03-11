package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xiaonanln/lockd/raft"
)

var (
	raftInstances []*DemoRaftInstance
)

const (
	INSTANCE_NUM = 3

	PERIOD_DURATION = time.Second * 3
)

type TimePeriod struct {
	startTime       time.Time
	duration        time.Duration
	instanceHealthy map[int]*InstanceHealthy
}

type InstanceHealthy struct {
	Crash       bool
	NetworkDown bool
	SendDelay   time.Duration
	RecvDelay   time.Duration
}

func newInstanceBroken() *InstanceHealthy {
	healthy := *InstanceFine

	if rand.Float32() < 0.5 {
		healthy.Crash = true
		return &healthy
	}

	if rand.Float32() < 0.8 {
		healthy.NetworkDown = true
		return &healthy
	}

	healthy.SendDelay = time.Millisecond * time.Duration(rand.Intn(2000))
	healthy.RecvDelay = time.Millisecond * time.Duration(rand.Intn(2000))
	return &healthy
}

func (h *InstanceHealthy) CanSend() bool {
	return !(h.Crash || h.NetworkDown)
}

func (h *InstanceHealthy) CanRecv() bool {
	return !(h.Crash || h.NetworkDown)
}

var (
	currentPeriod *TimePeriod = nil
	InstanceFine              = &InstanceHealthy{
		Crash:       false,
		NetworkDown: false,
		SendDelay:   0,
		RecvDelay:   0,
	}
)

func main() {
	ctx := context.Background()

	for i := 0; i < INSTANCE_NUM; i++ {
		ins := NewDemoRaftInstance(ctx, i)
		raftInstances = append(raftInstances, ins)
		ins.Raft = raft.NewRaft(ctx, INSTANCE_NUM, ins, ins)
	}

	verifyCounter := 0
	inputCounter := 0

	currentPeriod = newTimePeriod()

	applyTimePeriod := func() {
		for i := 0; i < INSTANCE_NUM; i++ {
			ins := raftInstances[i]
			ins.SetHealthy(currentPeriod.instanceHealthy[i])
		}
	}

	applyTimePeriod()
	for {
		randInsIdx := rand.Intn(INSTANCE_NUM)
		inputCounter = inputCounter + 1
		inputData := strconv.Itoa(inputCounter)
		r := raftInstances[randInsIdx].Raft
		r.Lock()
		if r.Mode() == raft.Leader {
			r.Input([]byte(inputData))
		}
		r.Unlock()

		time.Sleep(time.Microsecond * 10)
		verifyCounter += 1
		if verifyCounter%10000 == 0 {
			verifyCorrectness(raftInstances)
		}

		if currentPeriod.isTimeout() {
			currentPeriod = newTimePeriod()
			applyTimePeriod()
		}
	}

	<-ctx.Done()
}

func newTimePeriod() *TimePeriod {
	tp := &TimePeriod{
		startTime:       time.Now(),
		duration:        PERIOD_DURATION,
		instanceHealthy: map[int]*InstanceHealthy{},
	}

	maxBrokenNum := INSTANCE_NUM / 2
	brokenNum := rand.Intn(maxBrokenNum + 1)
	for i := 0; i < INSTANCE_NUM; i++ {
		brokenProb := float64(brokenNum) / float64((INSTANCE_NUM - i))
		if rand.Float64() < brokenProb {
			// this instance should be broken
			tp.instanceHealthy[i] = newInstanceBroken()
			brokenNum -= 1
		} else {
			// this instance should be healthy
			tp.instanceHealthy[i] = InstanceFine
		}
	}
	return tp
}

func (tp *TimePeriod) isBroken(idx int) bool {
	_, ok := tp.instanceHealthy[idx]
	return ok
}

func (tp *TimePeriod) isTimeout() bool {
	return time.Now().After(tp.startTime.Add(tp.duration))
}

var lastLeaderCommitIndex = raft.InvalidLogIndex

func verifyCorrectness(instances []*DemoRaftInstance) {
	// verify the correctness of Raft algorithm
	// lock all Raft instances before
	lock := func() {
		for i := 0; i < INSTANCE_NUM; i++ {
			raftInstances[i].Raft.Lock()
		}
	}

	unlock := func() {
		for i := 0; i < INSTANCE_NUM; i++ {
			raftInstances[i].Raft.Unlock()
		}
	}

	lock()
	defer unlock()

	leader := findLeader()
	followers := []*raft.Raft{}
	for i := 0; i < INSTANCE_NUM; i++ {
		r := raftInstances[i].Raft
		r.VerifyCorrectness()
		if r != leader {
			followers = append(followers, r)
		}
	}

	if leader == nil {
		log.Printf("leader not found")
	} else {
		log.Printf("found leader: %v, followers: %v", leader, followers)
	}

	// 监查每个Raft实例，确定所有Commit部分都完全相同。
	if leader == nil {
		return
	}

	leaderCommitIndex := leader.CommitIndex
	assert.GreaterOrEqual(leader.Logger, leaderCommitIndex, lastLeaderCommitIndex) // commit index should only grow
	lastLeaderCommitIndex = leaderCommitIndex

	isAllAppliedLogIndexSame := true    // determine if all rafts has same LastAppliedIndex
	minLogIndex := raft.InvalidLogIndex // to find the first log logIndex that <= leader.CommitIndex and exists on all raft instances
	// other raft instances should not have larger commit logIndex
	for i := 0; i < INSTANCE_NUM; i++ {
		r := raftInstances[i].Raft
		//assert.LessOrEqual(r.Logger, r.CommitIndex, leaderCommitIndex) // not necessary so
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
		assert.Equal(leader.Logger, logIndex, leaderLog.Index)
		logTerm := leaderLog.Term
		logData := leaderLog.Data

		for i := 0; i < INSTANCE_NUM; i++ {
			r := raftInstances[i].Raft
			if r != leader && logIndex <= r.CommitIndex {
				followerLog := r.LogList.GetLog(logIndex)
				assert.Equal(r.Logger, logIndex, followerLog.Index)
				assert.Equal(r.Logger, logTerm, followerLog.Term)
				assert.Equal(r.Logger, logData, followerLog.Data)
			}
		}
	}

	// make sure all state machines are consistent
	if isAllAppliedLogIndexSame {
		for i := 0; i < INSTANCE_NUM-1; i++ {
			assert.True(leader.Logger, instances[i].StateMachineEquals(instances[i+1]))
		}
	}

	log.Printf("Verify correctness ok: leader's commit logIndex = %v, check entries range: %v ~ %v", leaderCommitIndex, minLogIndex, leaderCommitIndex)
}

func findLeader() *raft.Raft {
	leaders := []int{}
	for i := 0; i < INSTANCE_NUM; i++ {
		if raftInstances[i].Raft.Mode() == raft.Leader {
			leaders = append(leaders, i)
		}
	}

	if len(leaders) == 1 {
		return raftInstances[leaders[0]].Raft
	} else if len(leaders) == 0 {
		return nil
	} else {
		// found multiple leaders, use the one with highest commited index as the real leader
		bestLeader := leaders[0]
		for i := 1; i < len(leaders); i++ {
			if raftInstances[leaders[i]].Raft.CommitIndex > raftInstances[bestLeader].Raft.CommitIndex {
				bestLeader = leaders[i]
			}
		}
		return raftInstances[bestLeader].Raft
	}
}
