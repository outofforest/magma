package repository

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/config"
	"github.com/outofforest/magma/state/repository/format"
)

var pageSize = uint64(os.Getpagesize())

func newRepo(t *testing.T, dir string, partitionConfig config.PartitionConfig) (*Repository, string) {
	if dir == "" {
		dir = t.TempDir()
	}
	repo, err := Open(dir, pageSize, partitionConfig, 1024*1024)
	require.NoError(t, err)
	return repo, dir
}

func TestOpenFailsIfPageSizeIsInvalid(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	repo, err := Open(t.TempDir(), pageSize-1, config.PartitionConfig{}, 0)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestOpenFailsIfPageSizeIsZero(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	repo, err := Open(t.TempDir(), 0, config.PartitionConfig{}, 0)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestOpenFailsIfPageSizeIsTooSmall(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	repo, err := Open(t.TempDir(), maxHeaderSize, config.PartitionConfig{}, 0)
	requireT.Error(err)
	requireT.Nil(repo)
}

func TestPageCapacity(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	requireT.Equal(pageSize-maxHeaderSize, r.PageCapacity())
}

func TestCreateFailsIfTermIsZero(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(0, 0, 0)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateFirstTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "", config.PartitionConfig{})

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
		HeaderChecksum: 10947817979223171238,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-size)), data[size:])

	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
	}, r.files)
}

func TestCreateFollowingTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 10, 0)
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
		HeaderChecksum: 10947817979223171238,
	}, h)

	data, err = os.ReadFile(filepath.Join(dir, "1"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	h, size, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		Version:        0,
		FileIndex:      1,
		PreviousTerm:   1,
		Term:           2,
		NextIndex:      10,
		HeaderChecksum: 7738361760840593623,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(maxHeaderSize-size)), data[size:maxHeaderSize])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize)), data[maxHeaderSize:])

	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           2,
			NextIndex:      10,
			HeaderChecksum: 7738361760840593623,
		},
	}, r.files)
}

func TestCreateOnChangingVersion(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, dir := newRepo(t, "", config.PartitionConfig{})

	requireT.NoError(r.SetCurrentVersion(3))
	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.NoError(r.SetCurrentVersion(5))
	file, err = r.Create(2, 10, 0)
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
		Version:        3,
		Term:           1,
		HeaderChecksum: 461987110777793598,
	}, h)

	data, err = os.ReadFile(filepath.Join(dir, "1"))
	requireT.NoError(err)
	requireT.Len(data, int(pageSize))

	h, size, err := m.Unmarshal(id, data)
	requireT.NoError(err)
	requireT.Equal(&format.Header{
		Version:        5,
		FileIndex:      1,
		PreviousTerm:   1,
		Term:           2,
		NextIndex:      10,
		HeaderChecksum: 11062424617275434131,
	}, h)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(maxHeaderSize-size)), data[size:maxHeaderSize])
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize-maxHeaderSize)), data[maxHeaderSize:])

	requireT.Equal([]*format.Header{
		{
			Version:        3,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 461987110777793598,
		},
		{
			Version:        5,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           2,
			NextIndex:      10,
			HeaderChecksum: 11062424617275434131,
		},
	}, r.files)
}

func TestCreateTriggersUpgrade(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})
	requireT.NotEmpty(r.upgradeCh)

	<-r.upgradeCh

	_, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.Empty(r.upgradeCh)

	_, err = r.Create(2, 5, 0)
	requireT.NoError(err)
	requireT.Empty(r.upgradeCh)

	requireT.NoError(r.SetCurrentVersion(1))
	_, err = r.Create(2, 10, 0)
	requireT.NoError(err)
	requireT.NotEmpty(r.upgradeCh)

	<-r.upgradeCh

	requireT.NoError(r.SetCurrentVersion(2))
	_, err = r.Create(3, 20, 0)
	requireT.NoError(err)
	requireT.NotEmpty(r.upgradeCh)

	requireT.NoError(r.SetCurrentVersion(3))
	_, err = r.Create(4, 30, 0)
	requireT.NoError(err)
	requireT.NotEmpty(r.upgradeCh)
}

func TestCreateFailsOnPastTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(2, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(1, 10, 0)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateFailsOnPastNextIndex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 100, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(1, 100, 0)
	requireT.Error(err)
	requireT.Nil(file)

	file, err = r.Create(1, 10, 0)
	requireT.Error(err)
	requireT.Nil(file)
}

func TestCreateHeader(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 2, 0)
	requireT.NoError(err)
	requireT.Equal(format.Header{
		Version:        0,
		FileIndex:      0,
		PreviousTerm:   0,
		Term:           1,
		NextIndex:      2,
		HeaderChecksum: 2412368541892341534,
	}, file.Header())

	requireT.NotNil(file)
	requireT.NoError(file.Close())

	file, err = r.Create(2, 3, 0)
	requireT.NoError(err)
	requireT.Equal(format.Header{
		Version:        0,
		FileIndex:      1,
		PreviousTerm:   1,
		Term:           2,
		NextIndex:      3,
		HeaderChecksum: 6169776427810801835,
	}, file.Header())
	requireT.NotNil(file)
	requireT.NoError(file.Close())
}

func TestRevert(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 4, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(2, 8, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 12, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(5, 16, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(6, 20, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	lastTerm := r.Revert(21)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(6, r.nextFileIndex)
	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           2,
			NextIndex:      4,
			HeaderChecksum: 3883651765052643611,
		},
		{
			Version:        0,
			FileIndex:      2,
			PreviousTerm:   2,
			Term:           2,
			NextIndex:      8,
			HeaderChecksum: 18128662202108172676,
		},
		{
			Version:        0,
			FileIndex:      3,
			PreviousTerm:   2,
			Term:           5,
			NextIndex:      12,
			HeaderChecksum: 11759293431110518964,
		},
		{
			Version:        0,
			FileIndex:      4,
			PreviousTerm:   5,
			Term:           5,
			NextIndex:      16,
			HeaderChecksum: 14043145995855947347,
		},
		{
			Version:        0,
			FileIndex:      5,
			PreviousTerm:   5,
			Term:           6,
			NextIndex:      20,
			HeaderChecksum: 6947321830094343509,
		},
	}, r.files)

	lastTerm = r.Revert(20)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(6, r.nextFileIndex)
	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           2,
			NextIndex:      4,
			HeaderChecksum: 3883651765052643611,
		},
		{
			Version:        0,
			FileIndex:      2,
			PreviousTerm:   2,
			Term:           2,
			NextIndex:      8,
			HeaderChecksum: 18128662202108172676,
		},
		{
			Version:        0,
			FileIndex:      3,
			PreviousTerm:   2,
			Term:           5,
			NextIndex:      12,
			HeaderChecksum: 11759293431110518964,
		},
		{
			Version:        0,
			FileIndex:      4,
			PreviousTerm:   5,
			Term:           5,
			NextIndex:      16,
			HeaderChecksum: 14043145995855947347,
		},
	}, r.files)

	lastTerm = r.Revert(16)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(6, r.nextFileIndex)
	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           2,
			NextIndex:      4,
			HeaderChecksum: 3883651765052643611,
		},
		{
			Version:        0,
			FileIndex:      2,
			PreviousTerm:   2,
			Term:           2,
			NextIndex:      8,
			HeaderChecksum: 18128662202108172676,
		},
		{
			Version:        0,
			FileIndex:      3,
			PreviousTerm:   2,
			Term:           5,
			NextIndex:      12,
			HeaderChecksum: 11759293431110518964,
		},
	}, r.files)

	lastTerm = r.Revert(5)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(6, r.nextFileIndex)
	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           2,
			NextIndex:      4,
			HeaderChecksum: 3883651765052643611,
		},
	}, r.files)

	lastTerm = r.Revert(0)
	requireT.EqualValues(0, lastTerm)
	requireT.EqualValues(6, r.nextFileIndex)
	requireT.Equal([]*format.Header{}, r.files)
}

func TestRevertAndCreate(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(3, 4, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r.Create(4, 8, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r.nextFileIndex)

	lastTerm := r.Revert(5)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, r.nextFileIndex)

	file, err = r.Create(5, 16, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(4, r.nextFileIndex)

	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           3,
			NextIndex:      4,
			HeaderChecksum: 7084138438231326388,
		},
		{
			Version:        0,
			FileIndex:      3,
			PreviousTerm:   3,
			Term:           5,
			NextIndex:      16,
			HeaderChecksum: 8859678039726028011,
		},
	}, r.files)
}

func TestRevertAndOpen(t *testing.T) {
	requireT := require.New(t)
	r1, dir := newRepo(t, "", config.PartitionConfig{})

	file, err := r1.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(3, 4, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	file, err = r1.Create(4, 8, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(3, r1.nextFileIndex)

	lastTerm := r1.Revert(5)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, r1.nextFileIndex)

	file, err = r1.Create(5, 16, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())
	requireT.EqualValues(4, r1.nextFileIndex)

	r2, _ := newRepo(t, dir, config.PartitionConfig{})
	requireT.EqualValues(4, r2.nextFileIndex)
	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           3,
			NextIndex:      4,
			HeaderChecksum: 7084138438231326388,
		},
		{
			Version:        0,
			FileIndex:      3,
			PreviousTerm:   3,
			Term:           5,
			NextIndex:      16,
			HeaderChecksum: 8859678039726028011,
		},
	}, r2.files)

	requireT.Equal([]*format.Header{
		{
			Version:        0,
			FileIndex:      0,
			Term:           1,
			HeaderChecksum: 10947817979223171238,
		},
		{
			Version:        0,
			FileIndex:      1,
			PreviousTerm:   1,
			Term:           3,
			NextIndex:      4,
			HeaderChecksum: 7084138438231326388,
		},
		{
			Version:        0,
			FileIndex:      3,
			PreviousTerm:   3,
			Term:           5,
			NextIndex:      16,
			HeaderChecksum: 8859678039726028011,
		},
	}, r1.files)
}

func TestLastTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	requireT.Zero(r.LastTerm())

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.EqualValues(1, r.LastTerm())

	file, err = r.Create(3, 1, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	requireT.EqualValues(3, r.LastTerm())
}

func TestPreviousTerm(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	requireT.Zero(r.PreviousTerm(0))
	requireT.Zero(r.PreviousTerm(100))

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	term, nextIndex := r.PreviousTerm(0)
	requireT.Zero(term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(1)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(100)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)

	file, err = r.Create(2, 50, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	term, nextIndex = r.PreviousTerm(0)
	requireT.Zero(term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(1)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(49)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(50)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(51)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(100)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(200)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)

	file, err = r.Create(2, 100, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	term, nextIndex = r.PreviousTerm(0)
	requireT.Zero(term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(1)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(49)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(50)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(51)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(99)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(100)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(200)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)

	file, err = r.Create(4, 150, 0)
	requireT.NoError(err)
	requireT.NotNil(file)
	requireT.NoError(file.Close())

	term, nextIndex = r.PreviousTerm(0)
	requireT.Zero(term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(1)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(49)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(50)
	requireT.EqualValues(1, term)
	requireT.Zero(nextIndex)
	term, nextIndex = r.PreviousTerm(51)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(99)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(100)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(150)
	requireT.EqualValues(2, term)
	requireT.EqualValues(50, nextIndex)
	term, nextIndex = r.PreviousTerm(151)
	requireT.EqualValues(4, term)
	requireT.EqualValues(150, nextIndex)
	term, nextIndex = r.PreviousTerm(200)
	requireT.EqualValues(4, term)
	requireT.EqualValues(150, nextIndex)
}

func TestOpenCurrent(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Close())

	file, err = r.Create(3, 1, 0)
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
		Version:        0,
		FileIndex:      1,
		NextIndex:      1,
		PreviousTerm:   1,
		Term:           3,
		HeaderChecksum: 12571077618844824784,
	}, file.Header())

	var b [3]byte
	_, err = io.ReadFull(file.Reader(), b[:])
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02, 0x03}, b[:])

	requireT.NoError(file.Close())
}

func TestOpenCurrentReturnsNilIfRepoIsEmpty(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.OpenCurrent()
	requireT.NoError(err)
	requireT.Nil(file)
}

func TestOpenByIndex(t *testing.T) {
	requireT := require.New(t)
	r, dir := newRepo(t, "", config.PartitionConfig{})
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

	r, _ = newRepo(t, dir, config.PartitionConfig{})
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

func TestInitReader(t *testing.T) {
	requireT := require.New(t)
	r, dir := newRepo(t, "", config.PartitionConfig{})

	_, err := r.Create(1, 0, 0)
	requireT.NoError(err)

	requireT.NoError(r.SetCurrentVersion(3))
	_, err = r.Create(2, 10, 12)
	requireT.NoError(err)

	requireT.NoError(os.WriteFile(filepath.Join(dir, "1.init"), []byte{0x01, 0x02}, 0o600))

	_, _, _, err = r.InitReader(2)
	requireT.Error(err)

	_, _, _, err = r.InitReader(4)
	requireT.Error(err)

	nextIndex, reader, size, err := r.InitReader(3)
	requireT.NoError(err)
	defer reader.Close()
	requireT.EqualValues(10, nextIndex)
	requireT.NotNil(r)
	requireT.EqualValues(2, size)

	initData, err := io.ReadAll(reader)
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02}, initData)
}

func TestSync(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	requireT.NoError(file.Sync())
	requireT.NoError(file.Close())
}

func TestMapAndClose(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	_, err = file.Map()
	requireT.NoError(err)
	requireT.NoError(file.Close())
}

func TestMap(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

	file, err := r.Create(1, 0, 0)
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	requireT.Equal(bytes.Repeat([]byte{0x00}, int(pageSize)-maxHeaderSize), data)
	requireT.NoError(file.Close())
}

func TestCreateMapIsStored(t *testing.T) {
	requireT := require.New(t)
	r, _ := newRepo(t, "", config.PartitionConfig{})

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
	r, _ := newRepo(t, "", config.PartitionConfig{})

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
