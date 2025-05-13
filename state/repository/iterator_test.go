package repository

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x04, 0x05})
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x06})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6, 0)
	it := r.Iterator(tp, 0)

	reader, size, err := it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)
	requireT.EqualValues(3, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)
	requireT.EqualValues(2, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)
	requireT.EqualValues(1, size)

	requireT.NoError(it.Close())
}

func TestIteratorWithOffset2(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x04, 0x05})
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x06})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6, 0)
	it := r.Iterator(tp, 2)

	reader, size, err := it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data)
	requireT.EqualValues(1, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)
	requireT.EqualValues(2, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)
	requireT.EqualValues(1, size)

	requireT.NoError(it.Close())
}

func TestIteratorWithOffset3(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x04, 0x05})
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x06})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6, 0)
	it := r.Iterator(tp, 3)

	reader, size, err := it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)
	requireT.EqualValues(2, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)
	requireT.EqualValues(1, size)

	requireT.NoError(it.Close())
}

func TestIteratorWithOffset5(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x04, 0x05})
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x06})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(6, 0)
	it := r.Iterator(tp, 5)

	reader, size, err := it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06}, data)
	requireT.EqualValues(1, size)

	requireT.NoError(it.Close())
}

func TestIteratorWithTail(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x04, 0x05})
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5, 0)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x06})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(4, 0)
	it := r.Iterator(tp, 0)

	reader, size, err := it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)
	requireT.EqualValues(3, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x04}, data)
	requireT.EqualValues(1, size)

	tp.SetTail(7, 0)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x05}, data)
	requireT.EqualValues(1, size)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x06, 0x00}, data)
	requireT.EqualValues(2, size)

	requireT.NoError(it.Close())
}

func TestIteratorNonBlocking(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(3, 0)
	it := r.Iterator(tp, 0)

	reader, size, err := it.Reader(false)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)
	requireT.EqualValues(3, size)

	reader, size, err = it.Reader(false)
	requireT.NoError(err)
	requireT.Nil(reader)
	requireT.Zero(size)

	requireT.NoError(it.Close())
}

func TestIteratorHotEnd(t *testing.T) {
	requireT := require.New(t)

	r, _ := newRepo(t, "")
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	tp := NewTailProvider()
	tp.SetTail(3, 0)
	it := r.Iterator(tp, 0)

	reader, size, err := it.Reader(true)
	requireT.NoError(err)
	data, err = io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)
	requireT.EqualValues(3, size)

	tp.SetTail(3, 3)

	reader, size, err = it.Reader(true)
	requireT.NoError(err)
	requireT.Nil(reader)
	requireT.Zero(size)

	requireT.NoError(it.Close())
}
