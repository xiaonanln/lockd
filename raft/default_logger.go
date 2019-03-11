package raft

import (
	"encoding/json"

	"github.com/stretchr/testify/assert"
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

type assertLogger struct {
	logger Logger
}

func (al assertLogger) Errorf(format string, args ...interface{}) {
	al.logger.Fatalf(format, args...)
}

func MakeAssertLogger(logger Logger) assert.TestingT {
	return assertLogger{logger}
}
