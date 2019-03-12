package main

import (
	"context"
	"time"
)

const (
	INSTANCE_NUM = 3

	PERIOD_DURATION = time.Second * 3
)

func main() {
	ctx := context.Background()
	runner := newInstanceRunner(ctx, INSTANCE_NUM)
	runner.Run()
	<-ctx.Done()
}
