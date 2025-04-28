package repository

import (
	"io"
	"sync"

	"github.com/pkg/errors"

	magmatypes "github.com/outofforest/magma/types"
)

// NewTailProvider creates new tail provider.
func NewTailProvider() *TailProvider {
	return &TailProvider{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// TailProvider provides the valid tail index of the transaction stream.
type TailProvider struct {
	cond *sync.Cond
	tail magmatypes.Index
}

// Wait waits until transaction is available.
func (tp *TailProvider) Wait(current magmatypes.Index, closed *bool) (magmatypes.Index, error) {
	tp.cond.L.Lock()
	defer tp.cond.L.Unlock()

	for {
		if *closed {
			return 0, errors.New("iterator is closed")
		}

		if tp.tail > current {
			return tp.tail, nil
		}

		tp.cond.Wait()
	}
}

// Call executes a function in a safe region and unlocks blocked routines.
func (tp *TailProvider) Call(f func()) {
	tp.cond.L.Lock()
	defer tp.cond.L.Unlock()

	f()

	tp.cond.Broadcast()
}

// SetTail sets tail.
func (tp *TailProvider) SetTail(tail magmatypes.Index) {
	tp.cond.L.Lock()
	defer tp.cond.L.Unlock()

	tp.tail = tail
	tp.cond.Broadcast()
}

// NewIterator creates new iterator.
func NewIterator(provider *TailProvider, fileIterator *FileIterator, acknowledged magmatypes.Index) *Iterator {
	return &Iterator{
		provider:     provider,
		fileIterator: fileIterator,
		current:      acknowledged,
		files:        map[magmatypes.Index]*File{},
	}
}

// Iterator iterates over log.
type Iterator struct {
	provider     *TailProvider
	fileIterator *FileIterator

	readerValidUntil magmatypes.Index
	reader           io.Reader

	mu    sync.Mutex
	files map[magmatypes.Index]*File

	current magmatypes.Index
	closed  bool
}

// Reader returns next reader.
func (i *Iterator) Reader() (io.Reader, error) {
	tail, err := i.provider.Wait(i.current, &i.closed)
	if err != nil {
		return nil, err
	}

	if i.current >= i.readerValidUntil {
		reader, err := i.fileIterator.Next()
		if err != nil {
			return nil, err
		}
		i.reader = reader.Reader()
		i.readerValidUntil = reader.ValidUntil()

		i.mu.Lock()
		i.files[i.readerValidUntil] = reader
		i.mu.Unlock()
	}

	if tail > i.readerValidUntil {
		tail = i.readerValidUntil
	}

	size := tail - i.current
	i.current = tail

	return io.LimitReader(i.reader, int64(size)), nil
}

// Acknowledge acknowledges stream index which has been processed.
func (i *Iterator) Acknowledge(count magmatypes.Index) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	var toDelete []magmatypes.Index
	n := len(i.files)
	for o, f := range i.files {
		if o <= count {
			if toDelete == nil {
				toDelete = make([]magmatypes.Index, 0, n)
			}
			if err := f.Close(); err != nil {
				return err
			}
			toDelete = append(toDelete, o)
		}
		n--
	}

	for _, o := range toDelete {
		delete(i.files, o)
	}

	return nil
}

// Close closes the iterator.
func (i *Iterator) Close() error {
	i.provider.Call(func() {
		i.closed = true
	})

	i.mu.Lock()
	defer i.mu.Unlock()

	for _, file := range i.files {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}
