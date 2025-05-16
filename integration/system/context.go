package system

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/outofforest/logger"
)

// NewContext creates new context for testing.
func NewContext(t *testing.T) context.Context {
	return logger.WithLogger(t.Context(), logger.New(logger.DefaultConfig).With(zap.String("test", t.Name())))
}
