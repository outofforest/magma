package repository

import (
	"bytes"
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
	t.Parallel()

	requireT := require.New(t)

	repo, err := Open(t.TempDir(), pageSize-1)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestOpenFailsIfPageSizeIsZero(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	repo, err := Open(t.TempDir(), 0)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestOpenFailsIfPageSizeIsTooSmall(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	repo, err := Open(t.TempDir(), maxHeaderSize)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestPageCapacity(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "")

	requireT.Equal(pageSize-maxHeaderSize, r.PageCapacity())
}

func TestCreateFailsIfTermIsZero(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(0, 0, 0)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateFirstTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
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
		HeaderChecksum: 12640084365124082706,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-size)), data[size:])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 12640084365124082706,
			},
		},
	}, r.files)
}

func TestCreateFollowingTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 10, 11)
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
		HeaderChecksum: 12640084365124082706,
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
		HeaderChecksum:   3020817970091451091,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(maxHeaderSize-size)), data[size:maxHeaderSize])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize)), data[maxHeaderSize:])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 12640084365124082706,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:     1,
				PreviousChecksum: 11,
				Term:             2,
				NextLogIndex:     10,
				HeaderChecksum:   3020817970091451091,
			},
		},
	}, r.files)
}

func TestCreateFailsOnPastTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(2, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(1, 10, 11)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateHeader(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 2, 3)
	requireT.NoError(err)
	requireT.Equal(format.Header{
		PreviousTerm:     0,
		PreviousChecksum: 3,
		Term:             1,
		NextLogIndex:     2,
		HeaderChecksum:   6497390233957230078,
	}, file.Header())

	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 4)
	requireT.NoError(err)
	requireT.Equal(format.Header{
		PreviousTerm:     1,
		PreviousChecksum: 4,
		Term:             2,
		NextLogIndex:     3,
		HeaderChecksum:   17519384246767885116,
	}, file.Header())
	requireT.NotNil(file)
	requireT.NoError(file.Close())
}

func TestRevertToEqual(t *testing.T) {
	t.Parallel()
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 2, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 3, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 4, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 5, 0)
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
				HeaderChecksum: 12640084365124082706,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 17748549713685775268,
			},
		},
		{
			Index: 2,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           3,
				NextLogIndex:   2,
				HeaderChecksum: 16140727382978301460,
			},
		},
	}, r.files)
}

func TestRevertToLower(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 2, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 3, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 4, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 5, 0)
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
				HeaderChecksum: 12640084365124082706,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           2,
				NextLogIndex:   1,
				HeaderChecksum: 3021202660409141445,
			},
		},
		{
			Index: 2,
			Header: &format.Header{
				PreviousTerm:   2,
				Term:           2,
				NextLogIndex:   2,
				HeaderChecksum: 9092622013950781100,
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

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	_, _, err = r.Revert(1)
	requireT.Error(err)
}

func TestRevertAndCreate(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 2, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r.nextFileIndex)

	lastTerm, nextLogIndex, err := r.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(2, nextLogIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 3, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(4, r.nextFileIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 12640084365124082706,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 17748549713685775268,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextLogIndex:   3,
				HeaderChecksum: 17271735597156195553,
			},
		},
	}, r.files)
}

func TestRevertAndOpen(t *testing.T) {
	requireT := require.New(t)
	r1, dir := newRepo(t, "")

	file, err := r1.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(3, 1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(4, 2, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r1.nextFileIndex)

	lastTerm, nextLogIndex, err := r1.Revert(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(2, nextLogIndex)
	requireT.EqualValues(3, r1.nextFileIndex)

	file, err = r1.Create(5, 3, 0)
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
				HeaderChecksum: 12640084365124082706,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 17748549713685775268,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextLogIndex:   3,
				HeaderChecksum: 17271735597156195553,
			},
		},
	}, r2.files)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 12640084365124082706,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextLogIndex:   1,
				HeaderChecksum: 17748549713685775268,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextLogIndex:   3,
				HeaderChecksum: 17271735597156195553,
			},
		},
	}, r2.files)
}

func TestLastTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	requireT.Zero(r.LastTerm())

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.EqualValues(1, r.LastTerm())

	file, err = r.Create(3, 0, 0)
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

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(100))

	file, err = r.Create(2, 50, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(49))
	requireT.EqualValues(1, r.PreviousTerm(50))
	requireT.EqualValues(2, r.PreviousTerm(51))
	requireT.EqualValues(2, r.PreviousTerm(100))

	file, err = r.Create(4, 100, 0)
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

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.OpenCurrent()
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.Equal(format.Header{
		PreviousTerm:   1,
		Term:           3,
		HeaderChecksum: 18311182628384496241,
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

func TestSync(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Sync())
	requireT.NoError(file.Close())
}

func TestMapAndClose(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	_, err = file.Map()
	requireT.NoError(err)
	requireT.NoError(file.Close())
}

func TestMap(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize)-maxHeaderSize), data)
	requireT.NoError(file.Close())
}

func TestCreateMapIsStored(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	data[0] = 0x03

	data2 := make([]byte, 1)
	_, err = file.Reader().Read(data2)
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data2)

	requireT.NoError(file.Close())
}

func TestOpenCurrentMapIsStored(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.OpenCurrent()
	requireT.NoError(err)

	data, err := file.Map()
	requireT.NoError(err)
	data[0] = 0x03

	data2 := make([]byte, 1)
	_, err = file.Reader().Read(data2)
	requireT.NoError(err)
	requireT.Equal([]byte{0x03}, data2)

	requireT.NoError(file.Close())
}
