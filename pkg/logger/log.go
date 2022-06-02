package logger

// Package logging provides a replacement for the default golang log package. It wraps Uber's ZapCore
// logger in support for structured logging and additional logging features (e.g. logging levels)

import (
	"context"
	"event-data-pipeline/pkg/cli"
	"os"

	"github.com/alexflint/go-arg"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type correlationIdType int

const (
	requestIdKey correlationIdType = iota
	sessionIdKey
)

var (
	log          *zap.Logger
	filePath     string
	logToConsole bool
	logToFile    bool
	production   bool
)

// copied from init() funciton to control execution in runtime.
func Setup() {
	arg.MustParse(&cli.Args)

	filePath = cli.Args.LogfilePath
	logToFile = cli.Args.LoggingToFileEnabled
	production = !cli.Args.DebugEnabled

	var ec zapcore.EncoderConfig
	var level zapcore.Level

	// Apply one of the default encoder configs based on run-time environment (prod vs non-prod)
	if production {
		ec = zap.NewProductionEncoderConfig()
		level = zap.NewProductionConfig().Level.Level()
	} else {
		ec = zap.NewDevelopmentEncoderConfig()
		level = zap.NewDevelopmentConfig().Level.Level()
	}

	// Create a JSON encoder and customize date & time formatting
	encoder := zapcore.NewJSONEncoder(ec)
	ec.EncodeTime = zapcore.ISO8601TimeEncoder //The encoder can be customized for each output

	// Initialize logging to console
	var consoleEncoder zapcore.Encoder
	consoleEncoder = zapcore.NewConsoleEncoder(ec)

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)

	// Initialize logging to file if enabled
	var writerSyncer zapcore.WriteSyncer
	if logToFile {
		lumberJackLogger := &lumberjack.Logger{
			Filename:   filePath,
			MaxSize:    10,
			MaxBackups: 5,
			MaxAge:     30,
			Compress:   false,
		}
		writerSyncer = zapcore.AddSync(lumberJackLogger)

		core = zapcore.NewTee(core, zapcore.NewCore(encoder, writerSyncer, level))
	}

	// Include additional info in the log output
	log = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zap.FatalLevel))

	defer log.Sync() // flushes buffer, if any
	zap.ReplaceGlobals(log)
}

// WithRqId returns a context which knows its request ID
func WithRqId(ctx context.Context, rqId string) context.Context {
	return context.WithValue(ctx, requestIdKey, rqId)
}

// WithSessionId returns a context which knows its session ID
func WithSessionId(ctx context.Context, sessionId string) context.Context {
	return context.WithValue(ctx, sessionIdKey, sessionId)
}

// Logger returns a zap logger with as much context as possible
func Logger() *zap.SugaredLogger {
	return zap.L().Sugar()
}

// LoggerWithUniqueID returns a logger with the unique id automatically added as a field to the log output
func LoggerWithUniqueID(uid string) *zap.SugaredLogger {
	field := zap.Field{
		Key:    "uid",
		Type:   zapcore.StringType,
		String: uid,
	}
	return zap.L().With(field).Sugar()
}

// Println provides compatibility with golang log package. The input args will be concatenated into a single log message.
func Println(args ...interface{}) {
	Logger().Info(args...)
}

// Printf provides compatibility with golang log package. It uses fmt.Sprintf to log a templated message.
func Printf(format string, args ...interface{}) {
	Infof(format, args...)
}

// Fatal provides compatibility with golang log package. The input args will be concatenated into a single log message.
func Fatal(args ...interface{}) {
	Logger().Fatal(args...)
}

// Debugf uses fmt.Sprintf to log a templated message.
func Debugf(format string, args ...interface{}) {
	Logger().Debugf(format, args...)
}

// Debugw logs a message with some additional context. The variadic key-value pairs are treated as they are in With.
func Debugw(format string, args ...interface{}) {
	Logger().Debugw(format, args...)
}

// Infof uses fmt.Sprintf to log a templated message.
func Infof(format string, args ...interface{}) {
	Logger().Infof(format, args...)
}

// Infow logs a message with some additional context. The variadic key-value pairs are treated as they are in With.
func Infow(format string, args ...interface{}) {
	Logger().Infow(format, args...)
}

// Warnf uses fmt.Sprintf to log a templated message.
func Warnf(format string, args ...interface{}) {
	Logger().Warnf(format, args...)
}

// Warnw logs a message with some additional context. The variadic key-value pairs are treated as they are in With.
func Warnw(format string, args ...interface{}) {
	Logger().Warnw(format, args...)
}

// Errorf uses fmt.Sprintf to log a templated message.
func Errorf(format string, args ...interface{}) {
	Logger().Errorf(format, args...)
}

// Errorw logs a message with some additional context. The variadic key-value pairs are treated as they are in With.
func Errorw(format string, args ...interface{}) {
	Logger().Errorw(format, args...)
}

// Fatalf uses fmt.Sprintf to log a templated message, then calls os.Exit.
func Fatalf(format string, args ...interface{}) {
	Logger().Fatalf(format, args...)
}

// Fatalw logs a message with some additional context, then calls os.Exit. The variadic key-value pairs are treated as they are in With.
func Fatalw(format string, args ...interface{}) {
	Logger().Fatalw(format, args...)
}

// Panicf uses fmt.Sprintf to log a templated message, then panics.
func Panicf(format string, args ...interface{}) {
	Logger().Panicf(format, args...)
}

// Panicw logs a message with some additional context, then panics. The variadic key-value pairs are treated as they are in With.
func Panicw(format string, args ...interface{}) {
	Logger().Panicw(format, args...)
}
