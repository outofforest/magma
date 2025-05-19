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
	cond         *sync.Cond
	tail, hotEnd magmatypes.Index
}

// Wait waits until transaction is available.
func (tp *TailProvider) Wait(
	current magmatypes.Index,
	hotEnd *magmatypes.Index,
	block bool,
	closed *bool,
) (magmatypes.Index, bool, error) {
	tp.cond.L.Lock()
	defer tp.cond.L.Unlock()

	for {
		if *closed {
			return 0, false, errors.New("iterator is closed")
		}

		if tp.tail > current {
			return tp.tail, true, nil
		}

		if !block {
			return 0, false, nil
		}

		if tp.hotEnd > *hotEnd && current == tp.hotEnd {
			*hotEnd = tp.hotEnd
			return 0, false, nil
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
func (tp *TailProvider) SetTail(tail, hotEnd magmatypes.Index) {
	tp.cond.L.Lock()
	defer tp.cond.L.Unlock()

	tp.tail = tail
	tp.hotEnd = hotEnd
	tp.cond.Broadcast()
}

// Iterator iterates over files in the repository.
type Iterator struct {
	r         *Repository
	provider  *TailProvider
	fileIndex uint64
	offset    uint64

	current magmatypes.Index
	closed  bool
	hotEnd  magmatypes.Index

	mu          sync.Mutex
	currentFile *File
}

// Reader returns next reader.
func (i *Iterator) Reader(block bool) (io.Reader, uint64, error) {
	tail, valid, err := i.provider.Wait(i.current, &i.hotEnd, block, &i.closed)
	if err != nil {
		return nil, 0, err
	}

	if !valid {
		return nil, 0, nil
	}

	file, validUntil, err := i.file(i.current, tail)
	if err != nil {
		return nil, 0, err
	}

	size := validUntil - i.current
	i.current = validUntil

	return io.LimitReader(file.Reader(), int64(size)), uint64(size), nil
}

// Close closes iterator.
func (i *Iterator) Close() error {
	i.provider.Call(func() {
		i.closed = true
	})

	i.mu.Lock()
	defer i.mu.Unlock()

	if file := i.currentFile; file != nil {
		i.currentFile = nil
		return file.Close()
	}

	return nil
}

func (i *Iterator) file(current, tail magmatypes.Index) (*File, magmatypes.Index, error) {
	i.r.mu.RLock()
	defer i.r.mu.RUnlock()

	if len(i.r.files) == 0 {
		return nil, 0, errors.New("repository is empty")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	lastFileIndex := uint64(len(i.r.files)) - 1
	if i.fileIndex < lastFileIndex && current == i.r.files[i.fileIndex+1].Header.NextIndex {
		i.fileIndex++
		i.offset = 0
		if file := i.currentFile; file != nil {
			i.currentFile = nil
			if err := file.Close(); err != nil {
				return nil, 0, err
			}
		}
	}

	if i.currentFile == nil {
		var err error
		i.currentFile, err = i.r.open(i.fileIndex, i.offset)
		if err != nil {
			return nil, 0, err
		}
	}

	if i.fileIndex < lastFileIndex && i.r.files[i.fileIndex+1].Header.NextIndex < tail {
		return i.currentFile, i.r.files[i.fileIndex+1].Header.NextIndex, nil
	}

	return i.currentFile, tail, nil
}
