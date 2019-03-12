package main

import (
	"context"
	"encoding/json"
	"os"
)

type RunnerConfig struct {
	Quorum int `json:"quorum"`
	Num    int `json:"num"`
}

type RunnersConfig struct {
	Runners []RunnerConfig `json:"runners"`
}

func main() {
	ctx := context.Background()

	f, err := os.Open("runners.json")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	d := json.NewDecoder(f)
	var runnersConfig RunnersConfig
	err = d.Decode(&runnersConfig)
	if err != nil {
		panic(err)
	}

	for _, rcfg := range runnersConfig.Runners {
		for i := 0; i < rcfg.Num; i++ {
			runner := newInstanceRunner(ctx, rcfg.Quorum)
			go runner.Run()
		}
	}

	<-ctx.Done()
}
