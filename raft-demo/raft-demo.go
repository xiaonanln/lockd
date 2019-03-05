package main

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/xiaonanln/lockd/raft"
	"github.com/xiaonanln/lockd/raft-demo/demo"
	"log"
	"math/rand"
	"strconv"
	"time"
)

var (
	raftInstances []*demo.DemoRaftInstance
)

const (
	INSTANCE_NUM = 3
)

func main() {
	ctx := context.Background()

	for i := 0; i < INSTANCE_NUM; i++ {
		ins := demo.NewDemoRaftInstance(ctx, i)
		raftInstances = append(raftInstances, ins)
		ins.Raft = raft.NewRaft(ctx, INSTANCE_NUM, ins, ins)
	}

	verifyCounter := 0
	inputCounter := 0
	for {
		randInsIdx := rand.Intn(INSTANCE_NUM)
		inputCounter = inputCounter + 1
		inputData := strconv.Itoa(inputCounter)
		r := raftInstances[randInsIdx].Raft
		if r.Mode() == raft.Leader {
			r.Input([]byte(inputData))
		}

		time.Sleep(time.Microsecond * 100)
		verifyCounter += 1
		if verifyCounter%1000 == 0 {
			verifyCorrectness(raftInstances)
		}
	}

	<-ctx.Done()
}

func verifyCorrectness(instances []*demo.DemoRaftInstance) {
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
	isAllAppliedLogIndexSame := true    // determine if all rafts has same LastAppliedIndex
	minLogIndex := raft.InvalidLogIndex // to find the first log logIndex that <= leader.CommitIndex and exists on all raft instances
	// other raft instances should not have larger commit logIndex
	for i := 0; i < INSTANCE_NUM; i++ {
		r := raftInstances[i].Raft
		assert.LessOrEqual(r.Logger, r.CommitIndex, leaderCommitIndex)
		if minLogIndex < r.LogList.PrevLogIndex+1 {
			minLogIndex = r.LogList.PrevLogIndex + 1
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
	for i := 0; i < INSTANCE_NUM; i++ {
		if raftInstances[i].Raft.Mode() == raft.Leader {
			return raftInstances[i].Raft
		}
	}
	return nil // no leader, this can happen in this
}
