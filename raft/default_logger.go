package raft

import (
	"encoding/json"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	defaultLogCfg        zap.Config
	defaultLogger        *zap.Logger
	defaultSugaredLogger *zap.SugaredLogger
)

func init() {
	var err error
	cfgJson := []byte(`{
		"level": "debug",
		"outputPaths": ["stderr"],
		"errorOutputPaths": ["stderr"],
		"encoding": "console",
		"encoderConfig": {
			"messageKey": "message",
			"levelKey": "level",
			"levelEncoder": "lowercase"
		}
	}`)

	if err = json.Unmarshal(cfgJson, &defaultLogCfg); err != nil {
		panic(err)
	}
	defaultLogCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	defaultLogger, err := defaultLogCfg.Build()
	defaultSugaredLogger = defaultLogger.Sugar()
}
