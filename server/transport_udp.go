package server

import "github.com/xiaonanln/lockd/raft"

type TransportUDP struct {
}

func newTransportUDP(servers []string) *TransportUDP {
	t := &TransportUDP{}
	return t
}

func (tudp *TransportUDP) ID() raft.TransportID {
	return raft.InvalidTransportID
}

func (tudp *TransportUDP) Recv() <-chan raft.RecvRPCMessage {
	return nil
}

func (tudp *TransportUDP) Send(dstID raft.TransportID, msg raft.RPCMessage) {

}
func (tudp *TransportUDP) Broadcast(msg raft.RPCMessage) {

}
