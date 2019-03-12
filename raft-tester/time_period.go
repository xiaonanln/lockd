package main

import (
	"math/rand"
	"time"
)

type TimePeriod struct {
	startTime       time.Time
	duration        time.Duration
	instanceHealthy map[int]*InstanceHealthy
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
			tp.instanceHealthy[i] = newBrokenInstanceHealthy()
			brokenNum -= 1
		} else {
			// this instance should be healthy
			tp.instanceHealthy[i] = newNormalInstanceHealthy()
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
