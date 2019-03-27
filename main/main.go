package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/xiaonanln/lockd/server"
)

var args struct {
	advertiseAddr string
	listenAddr    string
	peerAddrs     []string
}

func main() {
	ctx := context.Background()
	parseArgs()
	log.Printf("args: %#v", args)

	s := server.NewServer(ctx, args.advertiseAddr, args.peerAddrs)
	s.Serve(args.listenAddr)
}

func parseArgs() {
	flag.StringVar(&args.advertiseAddr, "advertise", "localhost:25971", "set advertise address")
	flag.StringVar(&args.listenAddr, "listen", "localhost:25971", "set listen address")
	var peerAddrs string
	flag.StringVar(&peerAddrs, "peers", "", "set peer addresses (separated by ;)")
	flag.Parse()

	peers := []string{}
	for _, peer := range strings.Split(peerAddrs, ";") {
		peer = strings.TrimSpace(peer)
		if peer != "" {
			peers = append(peers, peer)
		}
	}
	args.peerAddrs = peers
}
