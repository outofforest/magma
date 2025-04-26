package repository

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/state/repository/format"
)

var pageSize = uint64(os.Getpagesize())

func newRepo(t *testing.T, dir string) (*Repository, string) {
	if dir == "" {
		dir = t.TempDir()
	}
	repo, err := Open(dir, pageSize)
	require.NoError(t, err)
	return repo, dir
}

func TestOpenFailsIfPageSizeIsInvalid(t *testing.T) {
	requireT := require.New(t)

	repo, err := Open(t.TempDir(), pageSize-1)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestOpenFailsIfPageSizeIsZero(t *testing.T) {
	requireT := require.New(t)

	repo, err := Open(t.TempDir(), 0)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestOpenFailsIfPageSizeIsTooSmall(t *testing.T) {
	requireT := require.New(t)

	repo, err := Open(t.TempDir(), maxHeaderSize)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestPageCapacity(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	requireT.Equal(pageSize-maxHeaderSize, r.PageCapacity())
}

func TestCreateFailsIfTermIsZero(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(0, 0, 0, nil)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateFailsIfRemainingDataSliceIsTooBig(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize+1)))
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateFirstTerm(t *testing.T) {
	requireT := require.New(t)
	r, dir := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	data, err := os.ReadFile(filepath.Join(dir, "0"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	m := format.NewMarshaller()
	id, err := m.ID(&format.Header{})
	requireT.NoError(err)

	h, size, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		Term:           1,
		HeaderChecksum: 1919979837750317581,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-size)), data[size:])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
	}, r.files)
}

func TestCreateTermWithRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, dir := newRepo(t, "")

	remainingData := []byte{0x01, 0x02, 0x03, 0x04}

	file, err := r.Create(1, 0, 0, remainingData)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	data, err := os.ReadFile(filepath.Join(dir, "0"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	m := format.NewMarshaller()
	id, err := m.ID(&format.Header{})
	requireT.NoError(err)

	h, size, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		Term:           1,
		NextTxOffset:   4,
		HeaderChecksum: 8988505189684172669,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(maxHeaderSize-size)), data[size:maxHeaderSize])
	requireT.Equal(remainingData, data[maxHeaderSize:maxHeaderSize+len(remainingData)])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize-uint64(len(remainingData)))),
		data[maxHeaderSize+len(remainingData):])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				NextTxOffset:   4,
				HeaderChecksum: 8988505189684172669,
			},
		},
	}, r.files)
}

func TestCreateFollowingTerm(t *testing.T) {
	requireT := require.New(t)
	r, dir := newRepo(t, "")

	remainingData := []byte{0x01, 0x02, 0x03, 0x04}

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 10, 11, remainingData)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	m := format.NewMarshaller()
	id, err := m.ID(&format.Header{})
	requireT.NoError(err)

	data, err := os.ReadFile(filepath.Join(dir, "0"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	h, _, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		Term:           1,
		HeaderChecksum: 1919979837750317581,
	}, h)

	data, err = os.ReadFile(filepath.Join(dir, "1"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	h, size, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		PreviousTerm:     1,
		PreviousChecksum: 11,
		Term:             2,
		NextLogIndex:     10,
		NextTxOffset:     4,
		HeaderChecksum:   646014842550638434,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(maxHeaderSize-size)), data[size:maxHeaderSize])
	requireT.Equal(remainingData, data[maxHeaderSize:maxHeaderSize+len(remainingData)])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize-uint64(len(remainingData)))),
		data[maxHeaderSize+len(remainingData):])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:     1,
				PreviousChecksum: 11,
				Term:             2,
				NextLogIndex:     10,
				NextTxOffset:     4,
				HeaderChecksum:   646014842550638434,
			},
		},
	}, r.files)
}

func TestCreateFailsOnPastTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(2, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(1, 10, 11, nil)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateHeader(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 2, 3, []byte{0x01, 0x02, 0x03, 0x04})
	requireT.Equal(format.Header{
		PreviousTerm:     0,
		PreviousChecksum: 3,
		Term:             1,
		NextLogIndex:     2,
		NextTxOffset:     4,
		HeaderChecksum:   2513786039398176540,
	}, file.Header())
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 4, []byte{0x01, 0x02, 0x03, 0x04, 0x05})
	requireT.Equal(format.Header{
		PreviousTerm:     1,
		PreviousChecksum: 4,
		Term:             2,
		NextLogIndex:     3,
		NextTxOffset:     5,
		HeaderChecksum:   12712223540869786415,
	}, file.Header())
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())
}

func TestRevertToEqual(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 1, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 2, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 3, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 4, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 5, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextLogIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 4167942253928074945,
			},
		},
		{
			Index: 2,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           3,
				NextLogIndex:   2,
				HeaderChecksum: 10777572332671751712,
			},
		},
	}, r.files)
}

func TestRevertToLower(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 1, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 2, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 3, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 4, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 5, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(3, nextLogIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           2,
				NextLogIndex:   1,
				HeaderChecksum: 6730890971626060212,
			},
		},
		{
			Index: 2,
			Header: &format.Header{
				PreviousTerm:   2,
				Term:           2,
				NextLogIndex:   2,
				HeaderChecksum: 3348030304094097037,
			},
		},
	}, r.files)
}

func TestRevertFailsIfEmpty(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	_, _, err := r.Revert(1)
	requireT.Error(err)
}

func TestRevertFailsIfNothingToRevert(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	_, _, err = r.Revert(1)
	requireT.Error(err)
}

func TestRevertAndCreate(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 1, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 2, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r.nextFileIndex)

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(2, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 3, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(4, r.nextFileIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 4167942253928074945,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextLogIndex:   3,
				HeaderChecksum: 11438105524433219139,
			},
		},
	}, r.files)
}

func TestRevertAndOpen(t *testing.T) {
	requireT := require.New(t)
	r1, dir := newRepo(t, "")

	file, err := r1.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(3, 1, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(4, 2, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r1.nextFileIndex)

	lastTerm, nextLogIndex, err := r1.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(2, nextLogIndex)
	requireT.EqualValues(3, r1.nextFileIndex)

	file, err = r1.Create(5, 3, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(4, r1.nextFileIndex)

	r2, _ := newRepo(t, dir)
	requireT.EqualValues(4, r2.nextFileIndex)
	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 4167942253928074945,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextLogIndex:   3,
				HeaderChecksum: 11438105524433219139,
			},
		},
	}, r2.files)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 1919979837750317581,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 4167942253928074945,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextLogIndex:   3,
				HeaderChecksum: 11438105524433219139,
			},
		},
	}, r2.files)
}

func TestLastTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	requireT.Zero(r.LastTerm())

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.EqualValues(1, r.LastTerm())

	file, err = r.Create(3, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.EqualValues(3, r.LastTerm())
}

func TestPreviousTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	requireT.Zero(r.PreviousTerm(0))
	requireT.Zero(r.PreviousTerm(100))

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(100))

	file, err = r.Create(2, 50, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(49))
	requireT.EqualValues(1, r.PreviousTerm(50))
	requireT.EqualValues(2, r.PreviousTerm(51))
	requireT.EqualValues(2, r.PreviousTerm(100))

	file, err = r.Create(4, 100, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(49))
	requireT.EqualValues(1, r.PreviousTerm(50))
	requireT.EqualValues(2, r.PreviousTerm(51))
	requireT.EqualValues(2, r.PreviousTerm(99))
	requireT.EqualValues(2, r.PreviousTerm(100))
	requireT.EqualValues(4, r.PreviousTerm(101))
	requireT.EqualValues(4, r.PreviousTerm(200))
}

func TestOpenCurrent(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 0, 0, nil)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.OpenCurrent()
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.Equal(format.Header{
		PreviousTerm:   1,
		Term:           3,
		HeaderChecksum: 7024084494774855656,
	}, file.Header())
	requireT.NoError(file.Close())
}

func TestOpenCurrentReturnsNilIfRepoIsEmpty(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.OpenCurrent()
	requireT.NoError(err)
	requireT.Nil(file)
}

func TestIteratorStartingFromOffset0(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(5, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 5, 0, []byte{0x07})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	it := r.Iterator(0)

	file1, err := it.Next()
	requireT.NoError(err)
	requireT.EqualValues(3, file1.ValidUntil())
	data, err := io.ReadAll(io.LimitReader(file1.Reader(), int64(file1.ValidUntil())))
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data)
	requireT.NoError(file1.Close())

	file2, err := it.Next()
	requireT.NoError(err)
	requireT.EqualValues(5, file2.ValidUntil())
	data, err = io.ReadAll(io.LimitReader(file2.Reader(), int64(file2.ValidUntil()-file1.ValidUntil())))
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)
	requireT.NoError(file2.Close())

	file3, err := it.Next()
	requireT.NoError(err)
	requireT.EqualValues(5+pageSize-maxHeaderSize, file3.ValidUntil())
	data, err = io.ReadAll(io.LimitReader(file3.Reader(), 1))
	requireT.NoError(err)
	requireT.Equal([]byte{0x07}, data)
	requireT.NoError(file3.Close())
}

func TestIteratorStartingFromOffset2(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(5, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 5, 0, []byte{0x07})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	it := r.Iterator(2)

	file1, err := it.Next()
	requireT.NoError(err)
	requireT.EqualValues(3, file1.ValidUntil())
	data, err := io.ReadAll(io.LimitReader(file1.Reader(), 1))
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data)
	requireT.NoError(file1.Close())

	file2, err := it.Next()
	requireT.NoError(err)
	requireT.EqualValues(5, file2.ValidUntil())
	data, err = io.ReadAll(io.LimitReader(file2.Reader(), int64(file2.ValidUntil()-file1.ValidUntil())))
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)
	requireT.NoError(file2.Close())

	file3, err := it.Next()
	requireT.NoError(err)
	requireT.EqualValues(5+pageSize-maxHeaderSize, file3.ValidUntil())
	data, err = io.ReadAll(io.LimitReader(file3.Reader(), 1))
	requireT.NoError(err)
	requireT.Equal([]byte{0x07}, data)
	requireT.NoError(file3.Close())
}

func TestIteratorStartingFromFile2(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(5, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 5, 0, []byte{0x07})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	it := r.Iterator(3)

	file2, err := it.Next()
	requireT.NoError(err)
	data, err := io.ReadAll(io.LimitReader(file2.Reader(), int64(file2.ValidUntil()-3)))
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05}, data)
	requireT.NoError(file2.Close())

	file3, err := it.Next()
	requireT.NoError(err)
	data, err = io.ReadAll(io.LimitReader(file3.Reader(), 1))
	requireT.NoError(err)
	requireT.Equal([]byte{0x07}, data)
	requireT.NoError(file3.Close())
}

func TestIteratorStartingFromFile2Offset2(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(5, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 5, 0, []byte{0x07})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	it := r.Iterator(4)

	file2, err := it.Next()
	requireT.NoError(err)
	data, err := io.ReadAll(io.LimitReader(file2.Reader(), int64(file2.ValidUntil()-4)))
	requireT.NoError(err)
	requireT.Equal([]byte{0x05}, data)
	requireT.NoError(file2.Close())

	file3, err := it.Next()
	requireT.NoError(err)
	data, err = io.ReadAll(io.LimitReader(file3.Reader(), 1))
	requireT.NoError(err)
	requireT.Equal([]byte{0x07}, data)
	requireT.NoError(file3.Close())
}

func TestIteratorStartingFromFile3(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, []byte{0x01, 0x02, 0x03})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 3, 0, []byte{0x04, 0x05})
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 5, 0, []byte{0x06})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(5, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 5, 0, []byte{0x07})
	requireT.NoError(err)
	requireT.NoError(file.Close())

	it := r.Iterator(5)

	file3, err := it.Next()
	requireT.NoError(err)
	data, err := io.ReadAll(io.LimitReader(file3.Reader(), 1))
	requireT.NoError(err)
	requireT.Equal([]byte{0x07}, data)
	requireT.NoError(file3.Close())
}

func TestIteratorFailsIfRepoIsEmpty(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	it := r.Iterator(0)
	f, err := it.Next()
	requireT.Error(err)
	requireT.Nil(f)
}

func TestSync(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Sync())
	requireT.NoError(file.Close())
}

func TestMapAndClose(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	_, err = file.Map()
	requireT.NoError(err)
	requireT.NoError(file.Close())
}

func TestMapWithoutRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize)-maxHeaderSize), data)
	requireT.EqualValues(len(data), file.ValidUntil())
	requireT.NoError(file.Close())
}

func TestMapWithRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	remainingData := []byte{0x01, 0x02}

	file, err := r.Create(1, 0, 0, remainingData)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	requireT.Equal(remainingData, data[:len(remainingData)])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize)-maxHeaderSize-len(remainingData)), data[len(remainingData):])
	requireT.Len(data, int(file.ValidUntil()))
	requireT.NoError(file.Close())
}

func TestCreateMapIsStoredWithoutRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.EqualValues(pageSize-maxHeaderSize, file.ValidUntil())
	data, err := file.Map()
	requireT.NoError(err)
	data[0] = 0x03

	data2 := make([]byte, 1)
	_, err = file.Reader().Read(data2)
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data2)

	requireT.NoError(file.Close())
}

func TestCreateMapIsStoredWithRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	remainingData := []byte{0x01, 0x02}

	file, err := r.Create(1, 0, 0, remainingData)
	requireT.NoError(err)
	requireT.EqualValues(pageSize-maxHeaderSize, file.ValidUntil())
	data, err := file.Map()
	requireT.NoError(err)
	data[len(remainingData)] = 0x03

	data2 := make([]byte, 3)
	_, err = file.Reader().Read(data2)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data2)

	requireT.NoError(file.Close())
}

func TestOpenCurrentMapIsStoredWithoutRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0, nil)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.OpenCurrent()
	requireT.NoError(err)
	requireT.EqualValues(pageSize-maxHeaderSize, file.ValidUntil())

	data, err := file.Map()
	requireT.NoError(err)
	data[0] = 0x03

	data2 := make([]byte, 1)
	_, err = file.Reader().Read(data2)
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data2)

	requireT.NoError(file.Close())
}

func TestOpenCurrentMapIsStoredWithRemainingData(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	remainingData := []byte{0x01, 0x02}

	file, err := r.Create(1, 0, 0, remainingData)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.OpenCurrent()
	requireT.NoError(err)
	requireT.EqualValues(pageSize-maxHeaderSize, file.ValidUntil())

	data, err := file.Map()
	requireT.NoError(err)
	data[len(remainingData)] = 0x03

	data2 := make([]byte, 3)
	_, err = file.Reader().Read(data2)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, data2)

	requireT.NoError(file.Close())
}
