package logprovider

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

// CreateDefaultZapLogger creates a logger with default zap configuration
func CreateDefaultZapLogger(level zapcore.Level) (*zap.Logger, error) {
	logCfg := DefaultZapLoggerConfig
	logCfg.Level = zap.NewAtomicLevelAt(level)
	c, err := logCfg.Build()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// DefaultZapLoggerConfig defines default zap logger configuration.
var DefaultZapLoggerConfig = zap.Config{
	Level: zap.NewAtomicLevelAt(ConvertToZapLevel(DefaultLogLevel)),

	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},

	Encoding: DefaultLogFormat,

	// copied from "zap.NewProductionEncoderConfig" with some updates
	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:       "ts",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder,

		// Custom EncodeTime function to ensure we match format and precision of historic capnslog timestamps
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02T15:04:05.999999Z0700"))
		},

		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},

	// Use "/dev/null" to discard all
	OutputPaths:      []string{"stderr"},
	ErrorOutputPaths: []string{"stderr"},
}
