package repository

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6)
	it := NewIterator(tp, r.Iterator(0), 0)

	reader, err := it.Reader()
	requireT.NoError(err)
	data, err := io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)

	requireT.NoError(it.Close())
}

func TestIteratorWithOffset2(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6)
	it := NewIterator(tp, r.Iterator(2), 2)

	reader, err := it.Reader()
	requireT.NoError(err)
	data, err := io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)

	requireT.NoError(it.Close())
}

func TestIteratorWithOffset3(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6)
	it := NewIterator(tp, r.Iterator(3), 3)

	reader, err := it.Reader()
	requireT.NoError(err)
	data, err := io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)

	requireT.NoError(it.Close())
}

func TestIteratorWithOffset5(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6)
	it := NewIterator(tp, r.Iterator(5), 5)

	reader, err := it.Reader()
	requireT.NoError(err)
	data, err := io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)

	requireT.NoError(it.Close())
}

func TestIteratorWithTail(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(4)
	it := NewIterator(tp, r.Iterator(0), 0)

	reader, err := it.Reader()
	requireT.NoError(err)
	data, err := io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04}, data)

	tp.SetTail(7)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x05}, data)

	reader, err = it.Reader()
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06, 0x00}, data)

	requireT.NoError(it.Close())
}
