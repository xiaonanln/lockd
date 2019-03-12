package main

import (
	"time"
)

const (
	PERIOD_DURATION = time.Second * 3
)

type TimePeriod struct {
	startTime       time.Time
	duration        time.Duration
	instanceHealthy map[int]*InstanceHealthy
}

func (tp *TimePeriod) isBroken(idx int) bool {
	_, ok := tp.instanceHealthy[idx]
	return ok
}

func (tp *TimePeriod) isTimeout() bool {
	return time.Now().After(tp.startTime.Add(tp.duration))
}
