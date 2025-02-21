package reactor

import "time"

// TimeSource defines an interface to retrieve the current time, which can be implemented
// by different sources of time, such as real-time and test-time sources.
type TimeSource interface {
	Now() time.Time
}

// TimeAdvancer is an interface to manipulate time by advancing it,
// typically useful in testing scenarios where simulating the passage of time is required.
type TimeAdvancer interface {
	Add(duration time.Duration) time.Time
}

// RealTimeSource is an implementation of the TimeSource interface that uses the actual current time.
type RealTimeSource struct{}

// Now returns the current time from the real-time clock.
func (r *RealTimeSource) Now() time.Time {
	return time.Now()
}

// TestTimeSource is an implementation of the TimeSource interface that allows
// setting a specific time, making it useful for testing scenarios.
type TestTimeSource struct {
	now time.Time
}

// Now returns the specific time set for the TestTimeSource. This makes it
// useful for scenarios where deterministic or controlled time behavior is
// required, such as testing time-dependent code.
func (t *TestTimeSource) Now() time.Time {
	return t.now
}

// Add advances the current time of the TestTimeSource by the specified duration.
// This method is helpful in testing scenarios where you need to simulate
// the passage of time.
func (t *TestTimeSource) Add(duration time.Duration) time.Time {
	t.now = t.now.Add(duration)
	return t.now
}
