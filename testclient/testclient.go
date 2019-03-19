package main

import (
	"context"

	"github.com/xiaonanln/lockd/client"
)

func main() {
	c, err := client.NewClient(context.Background(), "localhost:50051")
	if err != nil {
		panic(err)
	}

	ok, err := c.Lock("testkey")
	println(ok, err)
}
