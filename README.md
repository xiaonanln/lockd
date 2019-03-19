# lockd
Distributed Locking Service

## Raft and replicated state machines
Lockd uses Raft to implement replicated state machines. 
The locking service is a state-machine that handle client inputs in a determined way. 
So, 3 or 5 locking service instances consist a cluster to offer high availability. 

## Implementing Raft Features

Most of Raft features will be implemented. 

- [ ] Basic Raft Algorithm
    - [x] Leader election
    - [x] Log replication
    - [x] Safety
    - [ ] Persisted state (won't have)
    - [x] Server restarts
    - [ ] Leader transfer extension
    
- [ ] Cluster membership changes

- [ ] Log compaction
    - [x] Memory-Based Snapshots (partly)

- [ ] Client interaction
    - [ ] Finding the cluster
    - [ ] Routing requests to the leader
    - [ ] Implementing linearizable semantics
    - [ ] Processing read-only queries more efficiently

## Raft-tester
The raft-tester is used to test the correctness in simulation. 
Raft-tester runs multiple Raft Clusters to test the correctness of Raft algorhtm
with different values of quorum. 
