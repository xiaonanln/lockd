# lockd
Distributed Locking Service

## Raft and replicated state machines
Lockd uses Raft to implement replicated state machines. 
The locking service is a state-machine that handle client inputs in a determined way. 
So, 3 or 5 locking service instances consist a cluster to offer high availability. 

## Implementing Raft Features

Yes, I am using this project to learn Raft.

- [ ] Basic Raft Algorithm
    - [ ] Leader election
    - [ ] Log replication
    - [ ] Safety
    - [ ] Persisted state
    - [ ] Server restarts
    - [ ] Leader transfer extension
    
- [ ] Cluster membership changes

- [ ] Log compaction
    - [ ] Memory-Based Snapshots

- [ ] Client interaction
    - [ ] Finding the cluster
    - [ ] Routing requests to the leader
    - [ ] Implementing linearizable semantics
    - [ ] Processing read-only queries more efficiently

## Raft demo
The raft-demo is used to test the correctness in simulation. 
Raft-demo sends test data to the raft algorithm 
