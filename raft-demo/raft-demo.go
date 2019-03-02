package main

import (
	"context"
	"github.com/xiaonanln/lockd/raft"
	"github.com/xiaonanln/lockd/raft-demo/demo"
	"math/rand"
	"strconv"
	"time"
)

const (
	INSTANCE_NUM = 3
)

func main() {
	ctx := context.Background()

	raftInstances := []*demo.DemoRaftInstance{}

	for i := 0; i < INSTANCE_NUM; i++ {
		ins := demo.NewDemoRaftInstance(ctx, i)
		raftInstances = append(raftInstances, ins)
		raft.NewRaft(ctx, INSTANCE_NUM, ins)
	}

	verifyCounter := 0
	inputCounter := 0
	for {
		randInsIdx := rand.Intn(INSTANCE_NUM)
		inputCounter = inputCounter + 1
		inputData := strconv.Itoa(inputCounter)
		raftInstances[randInsIdx].Input([]byte(inputData))
		time.Sleep(time.Millisecond)

		verifyCounter += 1

	}

	<-ctx.Done()
}
