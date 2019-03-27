package main

import (
	"time"

	"github.com/xiaonanln/lockd/raft"
)

const (
	PERIOD_DURATION = time.Second * 3
)

type TimePeriod struct {
	startTime       time.Time
	duration        time.Duration
	instanceHealthy map[raft.TransportID]*InstanceHealthy
}

func (tp *TimePeriod) isBroken(id raft.TransportID) bool {
	_, ok := tp.instanceHealthy[id]
	return ok
}

func (tp *TimePeriod) isTimeout() bool {
	return time.Now().After(tp.startTime.Add(tp.duration))
}
