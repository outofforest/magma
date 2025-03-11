package iterator

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

// New creates new iterator.
func New(dir string, fileSize, acknowledged, available uint64) (*Iterator, error) {
	f, err := os.Open(filepath.Join(dir, "log"))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := f.Seek(int64(acknowledged), io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	i := &Iterator{
		dir:      dir,
		fileSize: fileSize,
		f:        f,
		head:     acknowledged,
		current:  acknowledged,
		tail:     available,
	}
	i.condData = sync.NewCond(&i.mu)
	return i, nil
}

// Iterator iterates over log.
type Iterator struct {
	dir      string
	fileSize uint64
	f        *os.File

	mu                  sync.Mutex
	condData            *sync.Cond
	head, current, tail uint64
	closed              bool
}

// Available sets the number of available bytes.
func (i *Iterator) Available(count uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()

	oldTail := i.tail
	i.tail = count
	if oldTail == i.current {
		i.condData.Signal()
	}
}

// Acknowledge acknowledges bytes received by the consumer.
func (i *Iterator) Acknowledge(count uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.head = count
	// TODO (wojciech): Close not needed files.
}

// Reader returns next reader.
func (i *Iterator) Reader() (io.Reader, error) {
	size, err := i.waitForData()
	if err != nil {
		return nil, err
	}

	return io.LimitReader(i.f, int64(size)), nil
}

// Close closes the iterator.
func (i *Iterator) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()

	_ = i.f.Close()
	i.closed = true
	i.condData.Signal()
}

func (i *Iterator) waitForData() (uint64, error) {
	i.condData.L.Lock()
	defer i.condData.L.Unlock()

	for {
		if i.closed {
			return 0, errors.New("iterator is closed")
		}

		if i.tail > i.current {
			size := i.tail - i.current
			i.current = i.tail
			return size, nil
		}

		i.condData.Wait()
	}
}
