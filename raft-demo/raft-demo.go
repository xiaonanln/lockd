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
		ins.Raft = raft.NewRaft(ctx, INSTANCE_NUM, ins)
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

		time.Sleep(time.Millisecond)
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
	for i := 0; i < INSTANCE_NUM; i++ {
		r := raftInstances[i].Raft
		if len(r.LogList) > 0 {
			assert.Equal(r.Logger, raft.LogIndex(1), r.LogList[0].Index)
		}
	}

	if leader == nil {
		return
	}

	leaderCommitIndex := leader.CommitIndex
	// other raft instances should not have larger commit index
	for i := 0; i < INSTANCE_NUM; i++ {
		r := raftInstances[i].Raft
		assert.LessOrEqual(r.Logger, r.CommitIndex, leaderCommitIndex)
	}

	// make sure all commited logs are exactly same
	for index := raft.LogIndex(1); index <= leaderCommitIndex; index++ {
		assert.Equal(leader.Logger, index, leader.LogList[index-1].Index)
		logTerm := leader.LogList[index-1].Term
		logData := leader.LogList[index-1].Data

		for i := 0; i < INSTANCE_NUM; i++ {
			r := raftInstances[i].Raft
			if r != leader && index <= r.CommitIndex {
				assert.Equal(r.Logger, index, r.LogList[index-1].Index)
				assert.Equal(r.Logger, logTerm, r.LogList[index-1].Term)
				assert.Equal(r.Logger, logData, r.LogList[index-1].Data)
			}
		}
	}

	log.Printf("Verify correctness ok: leader's commit index = %v", leaderCommitIndex)
}

func findLeader() *raft.Raft {
	for i := 0; i < INSTANCE_NUM; i++ {
		if raftInstances[i].Raft.Mode() == raft.Leader {
			return raftInstances[i].Raft
		}
	}
	return nil // no leader, this can happen in this
}
