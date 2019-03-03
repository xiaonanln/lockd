package raft

import "fmt"

type NotLeaderError struct {
	LeaderAddr string
}

func (e NotLeaderError) Error() string {
	return fmt.Sprintf("not leader, leader is %s", e.LeaderAddr)
}
