package reactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRealTimeSource(t *testing.T) {
	ts := &RealTimeSource{}
	time1 := ts.Now()
	time.Sleep(100 * time.Millisecond)
	time2 := ts.Now()

	require.True(t, time2.After(time1))
}

func TestTestTimeSource(t *testing.T) {
	requireT := require.New(t)

	ts := &TestTimeSource{}

	requireT.Equal(time.Time{}, ts.Now())
	requireT.Equal(time.Time{}, ts.Now())

	expectedTime := (time.Time{}).Add(time.Hour)
	requireT.Equal(expectedTime, ts.Add(time.Hour))
	requireT.Equal(expectedTime, ts.Now())
	requireT.Equal(expectedTime, ts.Now())
}
