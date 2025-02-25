package timeouts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTicker(t *testing.T) {
	requireT := require.New(t)

	tc := newTicker()
	requireT.False(tc.Ticking())

	tc.Start(time.Hour)
	requireT.True(tc.Ticking())

	tc.Stop()
	requireT.False(tc.Ticking())
}
