package log

import (
	"bufio"
	"fmt"
	"os"
	"time"

	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"github.com/sirupsen/logrus"
)

// 全局日志对象
var logger *logrus.Logger

// Fields Fields
type Fields map[string]interface{}

// InitLogger 初始化日志
func InitLogger(path, name, level string) {
	baseLogPath := fmt.Sprintf("%s%s", path, name)
	writer, err := rotatelogs.New(
		baseLogPath+"_%Y%m%d.log",
		rotatelogs.WithLinkName(baseLogPath),      // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(7*24*time.Hour),     // 文件最大保存时间
		rotatelogs.WithRotationTime(24*time.Hour), // 日志切割时间间隔
	)
	if err != nil {
		panic(fmt.Sprintf("config local file system logger error: %v", err))
	}

	logger = logrus.New()
	switch level {
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
		logger.SetOutput(os.Stderr)
	case "info":
		setNull(logger)
		logger.SetLevel(logrus.InfoLevel)
	case "warn":
		setNull(logger)
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		setNull(logger)
		logger.SetLevel(logrus.ErrorLevel)
	default:
		setNull(logger)
		logger.SetLevel(logrus.InfoLevel)
	}

	g := &logrus.TextFormatter{
		TimestampFormat:  "2006-01-02 15:04:05",
		ForceColors:      true,
		QuoteEmptyFields: true,
		FullTimestamp:    true,
	}

	lfHook := NewHook(WriterMap{
		logrus.DebugLevel: writer, // 为不同级别设置不同的输出目的
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
		logrus.PanicLevel: writer,
		logrus.TraceLevel: writer,
	}, g)
	logger.AddHook(lfHook)
}

// NewLogger NewLogger
func NewLogger() *logrus.Logger {
	return logger
}

func setNull(logger *logrus.Logger) {
	src, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Println("err", err)
	}
	writer := bufio.NewWriter(src)
	logger.SetOutput(writer)
}

// Debug Debug
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Debugf Debugf
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Info Info
func Info(args ...interface{}) {
	logger.Info(args...)
}

// Infof Infof
func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

// Infoln Infoln
func Infoln(args ...interface{}) {
	logger.Infoln(args...)
}

// Warn Warn
func Warn(arg ...interface{}) {
	logger.Warn(arg...)
}

// Warnf Warnf
func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

// Error Error
func Error(args ...interface{}) {
	logger.Error(args...)
}

// Errorf Errorf
func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

// Errorln Errorln
func Errorln(args ...interface{}) {
	logger.Errorln(args...)
}

// WithFields WithFields
func WithFields(fields Fields) *logrus.Entry {
	return logger.WithFields(logrus.Fields(fields))
}
