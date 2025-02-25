package timeouts

import "time"

func newTicker() *ticker {
	t := time.NewTicker(1000 * time.Hour)
	t.Stop()

	return &ticker{t: t}
}

type ticker struct {
	t       *time.Ticker
	ticking bool
}

func (t *ticker) Ticks() <-chan time.Time {
	return t.t.C
}

func (t *ticker) Start(interval time.Duration) {
	t.ticking = true
	t.t.Reset(interval)
}

func (t *ticker) Stop() {
	t.ticking = false
	t.t.Stop()
}

func (t *ticker) Ticking() bool {
	return t.ticking
}
