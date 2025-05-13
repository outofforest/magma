package system

import (
	"context"
	"testing"

	"github.com/outofforest/logger"
)

// NewContext creates new context for testing.
func NewContext(t *testing.T) context.Context {
	return logger.WithLogger(t.Context(), logger.New(logger.DefaultConfig))
}
