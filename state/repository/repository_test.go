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

	file, err := r.Create(0, 0)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateFirstTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "")

	file, err := r.Create(1, 0)
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
		HeaderChecksum: 11339396051936363247,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-size)), data[size:])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 11339396051936363247,
			},
		},
	}, r.files)
}

func TestCreateFollowingTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 10)
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
		HeaderChecksum: 11339396051936363247,
	}, h)

	data, err = os.ReadFile(filepath.Join(dir, "1"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	h, size, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		PreviousTerm:   1,
		Term:           2,
		NextIndex:      10,
		HeaderChecksum: 2115352002467121528,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(maxHeaderSize-size)), data[size:maxHeaderSize])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize)), data[maxHeaderSize:])

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 11339396051936363247,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           2,
				NextIndex:      10,
				HeaderChecksum: 2115352002467121528,
			},
		},
	}, r.files)
}

func TestCreateFailsOnPastTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(2, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(1, 10)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateHeader(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 2)
	requireT.NoError(err)
	requireT.Equal(format.Header{
		PreviousTerm:   0,
		Term:           1,
		NextIndex:      2,
		HeaderChecksum: 11725894641885870814,
	}, file.Header())

	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3)
	requireT.NoError(err)
	requireT.Equal(format.Header{
		PreviousTerm:   1,
		Term:           2,
		NextIndex:      3,
		HeaderChecksum: 9639843743847120848,
	}, file.Header())
	requireT.NotNil(file)
	requireT.NoError(file.Close())
}

func TestRevertTermsToEqual(t *testing.T) {
	t.Parallel()
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 1)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 2)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 3)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 4)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 5)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextIndex, err := r.RevertTerms(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 11339396051936363247,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextIndex:      1,
				HeaderChecksum: 1229607450597664475,
			},
		},
		{
			Index: 2,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           3,
				NextIndex:      2,
				HeaderChecksum: 10839254553578776291,
			},
		},
	}, r.files)
}

func TestRevertTermsToLower(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 1)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 2)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 3)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 4)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 5)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm, nextIndex, err := r.RevertTerms(3)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(3, nextIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 11339396051936363247,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           2,
				NextIndex:      1,
				HeaderChecksum: 15123016224832269261,
			},
		},
		{
			Index: 2,
			Header: &format.Header{
				PreviousTerm:   2,
				Term:           2,
				NextIndex:      2,
				HeaderChecksum: 10710426285935794252,
			},
		},
	}, r.files)
}

func TestRevertTermsFailsIfEmpty(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	_, _, err := r.RevertTerms(1)
	requireT.Error(err)
}

func TestRevertTermsFailsIfNothingToRevert(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	_, _, err = r.RevertTerms(1)
	requireT.Error(err)
}

func TestRevertTermsAndCreate(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 1)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 2)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r.nextFileIndex)

	lastTerm, nextIndex, err := r.RevertTerms(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(2, nextIndex)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 3)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(4, r.nextFileIndex)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 11339396051936363247,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextIndex:      1,
				HeaderChecksum: 1229607450597664475,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextIndex:      3,
				HeaderChecksum: 13274324415149675937,
			},
		},
	}, r.files)
}

func TestRevertTermsAndOpen(t *testing.T) {
	requireT := require.New(t)
	r1, dir := newRepo(t, "")

	file, err := r1.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(3, 1)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(4, 2)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r1.nextFileIndex)

	lastTerm, nextIndex, err := r1.RevertTerms(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(2, nextIndex)
	requireT.EqualValues(3, r1.nextFileIndex)

	file, err = r1.Create(5, 3)
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
				HeaderChecksum: 11339396051936363247,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextIndex:      1,
				HeaderChecksum: 1229607450597664475,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextIndex:      3,
				HeaderChecksum: 13274324415149675937,
			},
		},
	}, r2.files)

	requireT.Equal([]fileInfo{
		{
			Index: 0,
			Header: &format.Header{
				Term:           1,
				HeaderChecksum: 11339396051936363247,
			},
		},
		{
			Index: 1,
			Header: &format.Header{
				PreviousTerm:   1,
				Term:           3,
				NextIndex:      1,
				HeaderChecksum: 1229607450597664475,
			},
		},
		{
			Index: 3,
			Header: &format.Header{
				PreviousTerm:   3,
				Term:           5,
				NextIndex:      3,
				HeaderChecksum: 13274324415149675937,
			},
		},
	}, r2.files)
}

func TestLastTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	requireT.Zero(r.LastTerm())

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.EqualValues(1, r.LastTerm())

	file, err = r.Create(3, 0)
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

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(100))

	file, err = r.Create(2, 50)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.Zero(r.PreviousTerm(0))
	requireT.EqualValues(1, r.PreviousTerm(1))
	requireT.EqualValues(1, r.PreviousTerm(49))
	requireT.EqualValues(1, r.PreviousTerm(50))
	requireT.EqualValues(2, r.PreviousTerm(51))
	requireT.EqualValues(2, r.PreviousTerm(100))

	file, err = r.Create(4, 100)
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

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NotNil(file)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.OpenCurrent()
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.Equal(format.Header{
		PreviousTerm:   1,
		Term:           3,
		HeaderChecksum: 13861199811570477881,
	}, file.Header())

	var b [3]byte
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, b[:])

	requireT.NoError(file.Close())
}

func TestOpenCurrentReturnsNilIfRepoIsEmpty(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.OpenCurrent()
	requireT.NoError(err)
	requireT.Nil(file)
}

func TestOpenByIndex(t *testing.T) {
	requireT := require.New(t)
	r, dir := newRepo(t, "")
	file, err := r.Create(1, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x01, 0x02, 0x03})
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x04, 0x05})
	requireT.NoError(file.Close())

	file, err = r.Create(3, 5)
	requireT.NoError(err)
	data, err = file.Map()
	requireT.NoError(err)
	copy(data, []byte{0x06})
	requireT.NoError(file.Close())

	r, _ = newRepo(t, dir)
	var b [4]byte

	file, err = r.OpenByIndex(0)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03, 0x00}, b[:])
	requireT.NoError(file.Close())

	file, err = r.OpenByIndex(1)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x02, 0x03, 0x00, 0x00}, b[:])
	requireT.NoError(file.Close())

	file, err = r.OpenByIndex(2)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x03, 0x00, 0x00, 0x00}, b[:])
	requireT.NoError(file.Close())

	file, err = r.OpenByIndex(3)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x04, 0x05, 0x00, 0x00}, b[:])
	requireT.NoError(file.Close())

	file, err = r.OpenByIndex(4)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x05, 0x00, 0x00, 0x00}, b[:])
	requireT.NoError(file.Close())

	file, err = r.OpenByIndex(5)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x06, 0x00, 0x00, 0x00}, b[:])
	requireT.NoError(file.Close())

	file, err = r.OpenByIndex(6)
	requireT.NoError(err)
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x00, 0x00, 0x00, 0x00}, b[:])
	requireT.NoError(file.Close())
}

func TestSync(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	requireT.NoError(file.Sync())
	requireT.NoError(file.Close())
}

func TestMapAndClose(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	_, err = file.Map()
	requireT.NoError(err)
	requireT.NoError(file.Close())
}

func TestMap(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize)-maxHeaderSize), data)
	requireT.NoError(file.Close())
}

func TestCreateMapIsStored(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "")

	file, err := r.Create(1, 0)
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

	file, err := r.Create(1, 0)
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
