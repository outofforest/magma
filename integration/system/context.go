package system

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/outofforest/logger"
)

// newContext creates new context for testing.
func newContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(
		logger.WithLogger(t.Context(), logger.New(logger.DefaultConfig).With(zap.String("test", t.Name()))),
		2*time.Minute,
	)
	t.Cleanup(cancel)
	return ctx
}
