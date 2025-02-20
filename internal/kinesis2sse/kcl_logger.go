package kinesis2sse

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/vmware/vmware-go-kcl-v2/logger"
)

type slogKCLLogger struct {
	logger *slog.Logger // required
}

func (z *slogKCLLogger) Debugf(format string, args ...any) {
	z.logger.Debug(fmt.Sprintf(format, args...))
}

func (z *slogKCLLogger) Infof(format string, args ...any) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *slogKCLLogger) Warnf(format string, args ...any) {
	z.logger.Warn(fmt.Sprintf(format, args...))
}

func (z *slogKCLLogger) Errorf(format string, args ...any) {
	z.logger.Error(fmt.Sprintf(format, args...))
}

func (z *slogKCLLogger) Fatalf(format string, args ...any) {
	z.logger.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (z *slogKCLLogger) Panicf(format string, args ...any) {
	z.logger.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (z *slogKCLLogger) WithFields(keyValues logger.Fields) logger.Logger {
	newLogger := z.logger.With()
	for k, v := range keyValues {
		newLogger = newLogger.With(k, v)
	}

	return &slogKCLLogger{
		logger: newLogger,
	}
}

// NewKCLLogger converts a slog.Logger to a logger.Logger, as required by vmware-go-kcl-v2.
func NewKCLLogger(logger *slog.Logger) logger.Logger {
	return &slogKCLLogger{
		logger: logger,
	}
}
