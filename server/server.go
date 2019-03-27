/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//go:generate protoc -I ../pb --go_out=plugins=grpc:../pb ../pb/lockd.proto

package server

import (
	"context"
	"log"
	"net"

	"github.com/xiaonanln/lockd/raft"

	"github.com/xiaonanln/lockd/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const ()

// server is used to implement helloworld.GreeterServer.
type Server struct {
	ctx         context.Context
	quorum      int
	cancelCtx   context.CancelFunc
	grpcServer  *grpc.Server
	raft        *raft.Raft
	transport   *TransportUDP
	lockMachine *LockMatchine
}

func (s *Server) Lock(ctx context.Context, request *pb.LockRequest) (reply *pb.LockReply, err error) {
	return
}

func NewServer(ctx context.Context, advertiseAddr string, peers []string) *Server {
	grpcServer := grpc.NewServer()
	serverContext, serverCancel := context.WithCancel(ctx)
	quorum := len(peers) + 1

	s := &Server{
		ctx:         serverContext,
		cancelCtx:   serverCancel,
		quorum:      quorum,
		grpcServer:  grpcServer,
		lockMachine: newLockMachine(),
		transport:   newTransportUDP(peers),
	}
	s.raft = raft.NewRaft(serverContext, quorum, s.transport, s.lockMachine, []raft.TransportID{})
	pb.RegisterLockdServer(grpcServer, s)
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)
	return s
}

func (s *Server) Serve(listenAddr string) {

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Serving on %s...", listenAddr)
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
