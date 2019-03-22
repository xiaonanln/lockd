package server

import "github.com/xiaonanln/lockd/raft"

type TransportUDP struct {
}

func newTransportUDP(servers []string) *TransportUDP {
	t := &TransportUDP{}
	return t
}

func (tudp *TransportUDP) ID() int {
	return 0
}

func (tudp *TransportUDP) Recv() <-chan raft.RecvRPCMessage {
	return nil
}

func (tudp *TransportUDP) Send(instanceID int, msg raft.RPCMessage) {

}
func (tudp *TransportUDP) Broadcast(msg raft.RPCMessage) {

}
