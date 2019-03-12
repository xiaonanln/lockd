package main

import (
	"math/rand"
	"time"
)

type InstanceHealthy struct {
	Crash           bool
	NetworkDown     bool
	SendDelay       time.Duration
	RecvDelay       time.Duration
	MessageDropRate float32 // probability of message being dropped when sending
}

func newBrokenInstanceHealthy() *InstanceHealthy {
	healthy := InstanceHealthyPerfect

	if rand.Float32() < 0.5 {
		healthy.Crash = true
		return &healthy
	}

	if rand.Float32() < 0.5 {
		healthy.NetworkDown = true
		return &healthy
	}

	// not crash, not network total down, but has very high message drop rate
	healthy.MessageDropRate = 0.8
	return &healthy
}

func newNormalInstanceHealthy() *InstanceHealthy {
	healthy := InstanceHealthyPerfect
	healthy.MessageDropRate = NORMAL_MESSAGE_DROP_RATE_MIN + (NORMAL_MESSAGE_DROP_RATE_MAX-NORMAL_MESSAGE_DROP_RATE_MIN)*rand.Float32()
	return &healthy
}

func (h *InstanceHealthy) CanSend() bool {
	return !(h.Crash || h.NetworkDown)
}

func (h *InstanceHealthy) CanRecv() bool {
	return !(h.Crash || h.NetworkDown)
}

var (
	InstanceHealthyPerfect = InstanceHealthy{
		Crash:           false,
		NetworkDown:     false,
		SendDelay:       0,
		RecvDelay:       0,
		MessageDropRate: 0,
	}
)
