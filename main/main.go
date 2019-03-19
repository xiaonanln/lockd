package main

import "github.com/xiaonanln/lockd/server"

const (
	DefaultServeAddr = ":50051"
)

func main() {
	s := server.NewServer()
	s.Serve(DefaultServeAddr)
}
