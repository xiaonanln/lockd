package main

import (
	"context"
	"flag"

	"github.com/xiaonanln/lockd/server"
)

const (
	DefaultServeAddr = ":50051"
)

var args struct {
	advertiseAddr string
	peerAddrs     []string
}

func main() {
	ctx := context.Background()
	parseArgs()

	endpoints := []string{}
	s := server.NewServer(ctx, endpoints)
	s.Serve(DefaultServeAddr)
}

func parseArgs() {
	flag.StringVar(&args.advertiseAddr, "advertise-addr", "", "set advertise address")
	flag.Parse()
}
