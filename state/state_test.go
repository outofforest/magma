//nolint:lll
package state

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/varuint64"
)

func newState(t *testing.T, dir string) (*State, string) {
	if dir == "" {
		dir = t.TempDir()
	}
	repo, err := repository.Open(filepath.Join(dir, "repo"), 4*1024)
	require.NoError(t, err)
	em, err := events.Open(filepath.Join(dir, "events"))
	require.NoError(t, err)
	s, sCloser, err := New(repo, em)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sCloser())
	})
	return s, dir
}

func appendLog(requireT *require.Assertions, s *State, data ...byte) {
	_, _, err := s.Append(data, false, true)
	requireT.NoError(err)
}

func logEqual(requireT *require.Assertions, s *State, expectedLog ...byte) {
	it := s.repo.Iterator(0)
	var index rafttypes.Index
	buf := bytes.NewBuffer(nil)
	for index < s.nextLogIndex {
		file, err := it.Next()
		requireT.NoError(err)
		requireT.NotNil(file)
		limit := file.ValidUntil() - index
		if limit > s.nextLogIndex-index {
			limit = s.nextLogIndex - index
		}
		n, err := io.Copy(buf, io.LimitReader(file.Reader(), int64(limit)))
		requireT.NoError(err)
		index += rafttypes.Index(n)
		requireT.NoError(file.Close())
	}
	requireT.Equal(expectedLog, buf.Bytes())
}

func TestCurrentTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.Zero(s.CurrentTerm())

	requireT.NoError(s.SetCurrentTerm(1))
	requireT.EqualValues(1, s.CurrentTerm())

	requireT.NoError(s.SetCurrentTerm(10))
	requireT.EqualValues(10, s.CurrentTerm())

	requireT.Error(s.SetCurrentTerm(10))
	requireT.Zero(s.CurrentTerm())

	requireT.Error(s.SetCurrentTerm(9))
	requireT.Zero(s.CurrentTerm())

	requireT.Error(s.SetCurrentTerm(0))
	requireT.Zero(s.CurrentTerm())
}

func TestVoteFor(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	candidateID1 := types.ServerID(uuid.New())
	granted, err := s.VoteFor(candidateID1)
	requireT.Error(err)
	requireT.False(granted)

	requireT.NoError(s.SetCurrentTerm(1))

	granted, err = s.VoteFor(types.ZeroServerID)
	requireT.Error(err)
	requireT.False(granted)

	granted, err = s.VoteFor(candidateID1)
	requireT.NoError(err)
	requireT.True(granted)

	granted, err = s.VoteFor(candidateID1)
	requireT.NoError(err)
	requireT.True(granted)

	candidateID2 := types.ServerID(uuid.New())
	granted, err = s.VoteFor(candidateID2)
	requireT.NoError(err)
	requireT.False(granted)

	requireT.NoError(s.SetCurrentTerm(2))
	granted, err = s.VoteFor(candidateID2)
	requireT.NoError(err)
	requireT.True(granted)

	granted, err = s.VoteFor(candidateID1)
	requireT.NoError(err)
	requireT.False(granted)
}

func TestLastLogTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.EqualValues(0, s.LastLogTerm())

	requireT.NoError(s.SetCurrentTerm(1))
	appendLog(requireT, s, 0x01, 0x01)
	requireT.EqualValues(1, s.LastLogTerm())

	requireT.NoError(s.SetCurrentTerm(5))
	appendLog(requireT, s, 0x01, 0x02, 0x01, 0x04)
	requireT.EqualValues(4, s.LastLogTerm())
}

func TestNextLogIndex(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	requireT.EqualValues(0, s.NextLogIndex())

	appendLog(requireT, s, 0x01, 0x01)

	requireT.EqualValues(10, s.NextLogIndex())

	appendLog(requireT, s, 0x01, 0x02, 0x02, 0x01, 0x00)

	requireT.EqualValues(31, s.NextLogIndex())
}

func TestPreviousTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.EqualValues(0, s.PreviousTerm(1000))
	requireT.EqualValues(0, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	requireT.NoError(s.SetCurrentTerm(1))
	appendLog(requireT, s, 0x01, 0x01)
	requireT.EqualValues(1, s.PreviousTerm(1000))
	requireT.EqualValues(1, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	requireT.NoError(s.SetCurrentTerm(3))
	appendLog(requireT, s, 0x01, 0x03)
	requireT.EqualValues(3, s.PreviousTerm(1000))
	requireT.EqualValues(3, s.PreviousTerm(20))
	requireT.EqualValues(1, s.PreviousTerm(10))
	requireT.EqualValues(3, s.PreviousTerm(11))
	requireT.EqualValues(0, s.PreviousTerm(0))
}

func TestValidateErrorOnZeroNextLogIndex(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Validate(0, 1)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestValidateErrorOnZeroLastLogTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Validate(1, 0)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestValidateErrorIfCurrentTermNotSet(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	lastTerm, nextIndex, err := s.Validate(0, 0)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestValidateErrorIfOverwrittenInTheMiddleOfTheTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x02, 0x01, 0x00}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(21, nextIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x0a, 0x01, 0x00, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(11, 1)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestValidateNothingHappensIfPreviousIndexDoesNotExist1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Validate(1, 1)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestValidateNothingHappensIfPreviousIndexDoesNotExist2(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(11, 1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())
}

func TestValidateRevertWhenLastLogDoesNotMatch(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(31, 4)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())
}

func TestValidateRevertToNothing(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(10, 4)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())
}

func TestAppendTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(1))
	lastTerm, nextIndex, err := s.AppendTerm()
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)

	requireT.NoError(s.SetCurrentTerm(127))
	lastTerm, nextIndex, err = s.AppendTerm()
	requireT.NoError(err)
	requireT.EqualValues(127, lastTerm)
	requireT.EqualValues(20, nextIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x09, 0x7f, 0x4a, 0xc4, 0xe6, 0x85, 0xfc, 0x22, 0x51, 0xc9,
	)

	requireT.NoError(s.SetCurrentTerm(128))
	lastTerm, nextIndex, err = s.AppendTerm()
	requireT.NoError(err)
	requireT.EqualValues(128, lastTerm)
	requireT.EqualValues(31, nextIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x09, 0x7f, 0x4a, 0xc4, 0xe6, 0x85, 0xfc, 0x22, 0x51, 0xc9,
		0x0a, 0x80, 0x01, 0x6f, 0xe7, 0x13, 0x12, 0xa2, 0xfa, 0x25, 0xb1,
	)
}

func TestAppendErrorIfCurrentTermNotSet(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnInvalidTxSize(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(200))

	lastTerm, nextIndex, err := s.Append([]byte{0x80}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())
}

func TestAppendErrorIfTxSizeIsTooLow(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorIfTxSizeIsTooBig1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x02, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorIfTxSizeIsTooBig2(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x02, 0x01, 0x00, 0x02, 0x03}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(21, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0xa, 0x1, 0x0, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	)
}

func TestAppendErrorIfTxSizeIsZero(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x00}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnInvalidTermNumber(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(200))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x80}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())
}

func TestAppendErrorOnMissingTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnTermNumberAboveCurrentTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x7f}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())
}

func TestAppendErrorOnNewZeroTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x00}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfSameTermCreatedInSameOperation(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x01, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorIfSameTermCreatedInNextOperation(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorIfLowerTermCreatedInSameOperation(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x02, 0x01, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x02, 0xb, 0x8c, 0x68, 0x5e, 0x86, 0x90, 0x91, 0x4b,
	)
}

func TestAppendErrorIfLowerTermCreatedInNextOperation(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x02}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x02, 0xb, 0x8c, 0x68, 0x5e, 0x86, 0x90, 0x91, 0x4b,
	)
	requireT.EqualValues(2, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x01}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x02, 0xb, 0x8c, 0x68, 0x5e, 0x86, 0x90, 0x91, 0x4b,
	)
}

func TestAppendErrorIfOverwrittenWithExistingHigherTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x01, 0x02, 0x01, 0x03}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(30, nextIndex)
	requireT.EqualValues(30, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27,
		0x9, 0x3, 0x5b, 0xfb, 0x94, 0xd1, 0xe7, 0x4f, 0xf3, 0xc0,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(10, 1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x03}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorIfOverwrittenWithExistingSameTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x01, 0x02, 0x01, 0x03}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(30, nextIndex)
	requireT.EqualValues(30, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27,
		0x9, 0x3, 0x5b, 0xfb, 0x94, 0xd1, 0xe7, 0x4f, 0xf3, 0xc0,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(20, 2)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(20, nextIndex)
	requireT.EqualValues(20, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27,
	)
	requireT.EqualValues(2, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x03}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(20, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27,
	)
}

func TestAppendErrorIfNoTerm(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x02, 0x01, 0x00}, false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorTermMarkNotAllowed1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, false)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorTermMarkNotAllowed2(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x02}, false, false)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorTermMarkNotAllowed3(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(0, 0)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x02}, false, false)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorTermMarkNotAllowed4(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x02, 0x00, 0x00, 0x01, 0x01}, false, false)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(21, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0xa, 0x0, 0x0, 0x2d, 0xd4, 0x3a, 0x37, 0x13, 0xde, 0x55, 0xd4,
	)
}

func TestAppendErrorIfTermMarkExceedsTxSliceBoundary(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	tx := []byte{0x01, 0x01}
	lastTerm, nextIndex, err := s.Append(tx[:1], false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfTxExceedsTxSliceBoundary(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	tx := []byte{0x01, 0x01, 0x02, 0x01, 0x00}
	lastTerm, nextIndex, err := s.Append(tx[:3], false, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
}

func TestAppendErrorIfThereIsNoChecksum(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, true, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfChecksumIsInvalid(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x21,
	}, true, true)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendIfTransactionIsTooBig(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))
	_, _, err := s.Append([]byte{
		0x01, 0x01,
	}, false, true)
	requireT.NoError(err)

	b := make([]byte, s.repo.PageCapacity()+1)
	varuint64.Put(b, s.repo.PageCapacity()-1)
	lastTerm, nextIndex, err := s.Append(b, false, false)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
}

func TestAppendNilOnEmptyLog1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(nil, false, true)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendNilOnNonEmptyLog1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append(nil, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())
}

func TestAppendOnEmptyOnlyTerm1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())
}

func TestAppendOnEmptyOnlyTerm2(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x02}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x02, 0xb, 0x8c, 0x68, 0x5e, 0x86, 0x90, 0x91, 0x4b,
	)
	requireT.EqualValues(2, s.LastLogTerm())
}

func TestAppendOnEmptyLogWithTerm1(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x01, 0x02, 0x01, 0x00}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(21, nextIndex)
	requireT.EqualValues(21, s.nextLogIndex)
	logEqual(requireT, s,
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0xa, 0x1, 0x0, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	)
	requireT.EqualValues(1, s.LastLogTerm())
}

func TestAppendOnEmptyLogWithTerm2(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{0x01, 0x02, 0x02, 0x01, 0x00}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(21, nextIndex)
	requireT.EqualValues(21, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x2, 0xb, 0x8c, 0x68, 0x5e, 0x86, 0x90, 0x91, 0x4b,
		0xa, 0x1, 0x0, 0x39, 0xc9, 0xc0, 0x90, 0x51, 0x46, 0xe, 0xfa,
	)
	requireT.EqualValues(2, s.LastLogTerm())
}

func TestFirstChecksum(t *testing.T) {
	requireT := require.New(t)

	requireT.Equal(xxh3.Hash([]byte{0x09, 0x01}),
		binary.LittleEndian.Uint64([]byte{0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20}))
}

func TestSecondChecksum(t *testing.T) {
	requireT := require.New(t)

	requireT.Equal(xxh3.HashSeed([]byte{0x09, 0x02}, xxh3.Hash([]byte{0x09, 0x01})),
		binary.LittleEndian.Uint64([]byte{0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27}))
}

func TestHappyPath(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Validate(0, 0)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
	}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
	)
	requireT.EqualValues(2, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(31, 2)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
	)
	requireT.EqualValues(2, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x01, 0x03,
	}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	requireT.EqualValues(41, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(41, 3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	requireT.EqualValues(41, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append(nil, false, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	requireT.EqualValues(41, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(10, 1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x04, 0x03, 0x00, 0x01, 0x02}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(32, nextIndex)
	requireT.EqualValues(32, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
	)
	requireT.EqualValues(4, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(32, 4)
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(32, nextIndex)
	requireT.EqualValues(32, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
	)
	requireT.EqualValues(4, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x06}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(42, nextIndex)
	requireT.EqualValues(42, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
		0x9, 0x6, 0x88, 0x3, 0x62, 0x41, 0x99, 0x7b, 0xd8, 0x79,
	)
	requireT.EqualValues(6, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(10, 1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x01, 0x07, 0x02, 0x00, 0x00}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
	)
	requireT.EqualValues(7, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(31, 7)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
	)
	requireT.EqualValues(7, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{0x02, 0x01, 0x01}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(42, nextIndex)
	requireT.EqualValues(42, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
		0xa, 0x1, 0x1, 0x6, 0xbc, 0x3a, 0x14, 0x60, 0x1f, 0xb8, 0x97,
	)
	requireT.EqualValues(7, s.LastLogTerm())
}

func TestHappyPathWithChecksum(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Validate(0, 0)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Zero(s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
	}, true, true)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
	)
	requireT.EqualValues(2, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(31, 2)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
	)
	requireT.EqualValues(2, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	}, true, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	requireT.EqualValues(41, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(41, 3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	requireT.EqualValues(41, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append(nil, true, true)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(41, nextIndex)
	requireT.EqualValues(41, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
		0x9, 0x3, 0x18, 0x82, 0x23, 0x4e, 0xbf, 0x25, 0xeb, 0xab,
	)
	requireT.EqualValues(3, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(10, 1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
	}, true, true)
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(32, nextIndex)
	requireT.EqualValues(32, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
	)
	requireT.EqualValues(4, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(32, 4)
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(32, nextIndex)
	requireT.EqualValues(32, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
	)
	requireT.EqualValues(4, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x9, 0x6, 0x88, 0x3, 0x62, 0x41, 0x99, 0x7b, 0xd8, 0x79,
	}, true, true)
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(42, nextIndex)
	requireT.EqualValues(42, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x4, 0x96, 0x2b, 0x44, 0x35, 0x55, 0xc, 0x7a, 0xa7, 0xb, 0x0, 0x1, 0x2, 0xf, 0xe3, 0x4e, 0x6, 0x16, 0x50, 0x1f, 0xe5,
		0x9, 0x6, 0x88, 0x3, 0x62, 0x41, 0x99, 0x7b, 0xd8, 0x79,
	)
	requireT.EqualValues(6, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(10, 1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.EqualValues(10, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	)
	requireT.EqualValues(1, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
	}, true, true)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
	)
	requireT.EqualValues(7, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Validate(31, 7)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
	)
	requireT.EqualValues(7, s.LastLogTerm())

	lastTerm, nextIndex, err = s.Append([]byte{
		0xa, 0x1, 0x1, 0x6, 0xbc, 0x3a, 0x14, 0x60, 0x1f, 0xb8, 0x97,
	}, true, false)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(42, nextIndex)
	requireT.EqualValues(42, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x7, 0x26, 0xfa, 0x41, 0x7, 0xae, 0x89, 0x83, 0xb3, 0xa, 0x0, 0x0, 0x58, 0x1b, 0x97, 0x2d, 0x8d, 0xbb, 0x9a, 0x47,
		0xa, 0x1, 0x1, 0x6, 0xbc, 0x3a, 0x14, 0x60, 0x1f, 0xb8, 0x97,
	)
	requireT.EqualValues(7, s.LastLogTerm())
}

func TestAppendManyFiles(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append([]byte{
		0x01, 0x01,
	}, false, true)
	requireT.NoError(err)

	for range 300 {
		for j := range uint8(251) {
			_, _, err := s.Append([]byte{
				0x02, 0x01, j,
			}, false, false)
			requireT.NoError(err)
		}
	}

	it := s.repo.Iterator(0)
	readers := []io.Reader{}
	files := []*repository.File{}
	for range 270 {
		file, err := it.Next()
		requireT.NoError(err)
		readers = append(readers, file.Reader())
		files = append(files, file)
	}

	reader := io.MultiReader(readers...)

	b := make([]byte, 11)
	_, err = io.ReadFull(reader, b[:10])
	requireT.NoError(err)
	requireT.Equal([]byte{0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20}, b[:10])

	seed := binary.LittleEndian.Uint64(b[2:10])
	for range 300 {
		for j := range uint8(251) {
			_, err := io.ReadFull(reader, b)
			requireT.NoError(err)
			requireT.Equal([]byte{0x0a, 0x01, j}, b[:3])

			checksum := binary.LittleEndian.Uint64(b[3:])
			expectedChecksum := xxh3.HashSeed(b[:3], seed)
			requireT.Equal(expectedChecksum, checksum)

			seed = checksum
		}
	}
	for _, file := range files {
		requireT.NoError(file.Close())
	}

	it = s.repo.Iterator(0)
	for i := range uint64(270) {
		file, err := it.Next()
		requireT.NoError(err)

		requireT.EqualValues((i+1)*3072, file.ValidUntil(), i)

		var previousTerm rafttypes.Term
		if i > 0 {
			previousTerm = 1
		}
		h := file.Header()
		requireT.Equal(format.Header{
			PreviousTerm:     previousTerm,
			PreviousChecksum: h.PreviousChecksum,
			Term:             1,
			NextLogIndex:     rafttypes.Index(i * s.repo.PageCapacity()),
			NextTxOffset:     h.NextTxOffset,
			HeaderChecksum:   h.HeaderChecksum,
		}, h)

		reader := file.Reader().(*os.File)
		_, err = reader.Seek(int64(h.NextTxOffset), io.SeekCurrent)
		requireT.NoError(err)

		_, err = io.ReadFull(reader, b)
		requireT.NoError(err)
		size, n := varuint64.Parse(b)

		checksum := binary.LittleEndian.Uint64(b[n+size-8:])
		expectedChecksum := xxh3.HashSeed(b[:n+size-8], h.PreviousChecksum)

		requireT.Equal(expectedChecksum, checksum)

		requireT.NoError(file.Close())
	}
}

func TestAppendManyTerms(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(121))
	for j := range uint8(120) {
		_, _, err := s.Append([]byte{
			0x01, j + 1,
		}, false, true)
		requireT.NoError(err)
		requireT.EqualValues(j+1, s.highestTermSeen)
	}

	it := s.repo.Iterator(0)
	readers := []io.Reader{}
	files := []*repository.File{}
	for range 120 {
		file, err := it.Next()
		requireT.NoError(err)
		readers = append(readers, io.LimitReader(file.Reader(), 10))
		files = append(files, file)
	}

	reader := io.MultiReader(readers...)

	b := make([]byte, 10)
	var seed uint64
	for j := range uint8(120) {
		_, err := io.ReadFull(reader, b)
		requireT.NoError(err)
		requireT.Equal([]byte{0x09, j + 1}, b[:2])

		checksum := binary.LittleEndian.Uint64(b[2:])
		expectedChecksum := xxh3.HashSeed(b[:2], seed)
		requireT.Equal(expectedChecksum, checksum)

		seed = checksum
	}
	for _, file := range files {
		requireT.NoError(file.Close())
	}

	it = s.repo.Iterator(0)
	for i := range uint64(120) {
		file, err := it.Next()
		requireT.NoError(err)

		if i == 119 {
			requireT.EqualValues(4262, file.ValidUntil())
		} else {
			requireT.EqualValues((i+1)*10, file.ValidUntil(), i)
		}

		h := file.Header()
		requireT.Equal(format.Header{
			PreviousTerm:     rafttypes.Term(i),
			PreviousChecksum: h.PreviousChecksum,
			Term:             rafttypes.Term(i + 1),
			NextLogIndex:     rafttypes.Index(i * 10),
			NextTxOffset:     0,
			HeaderChecksum:   h.HeaderChecksum,
		}, h)

		reader := file.Reader().(*os.File)

		_, err = io.ReadFull(reader, b)
		requireT.NoError(err)

		checksum := binary.LittleEndian.Uint64(b[2:])
		expectedChecksum := xxh3.HashSeed(b[:2], h.PreviousChecksum)

		requireT.Equal(expectedChecksum, checksum)

		requireT.NoError(file.Close())
	}
}

func TestNew(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")
	candidate := types.ServerID(uuid.New())

	requireT.NoError(s1.SetCurrentTerm(121))
	granted, err := s1.VoteFor(candidate)
	requireT.NoError(err)
	requireT.True(granted)

	for j := range uint8(120) {
		_, _, err := s1.Append([]byte{
			0x01, j + 1,
		}, false, true)
		requireT.NoError(err)
	}

	s2, _ := newState(t, dir)
	requireT.EqualValues(121, s2.CurrentTerm())
	requireT.Equal(candidate, s2.evState.VotedFor)
	requireT.EqualValues(120, s2.LastLogTerm())
	requireT.EqualValues(120, s2.highestTermSeen)
	requireT.EqualValues(1200, s2.NextLogIndex())
	requireT.EqualValues(10, s2.nextLogIndexInFile)

	lastTerm, nextIndex, err := s2.Append([]byte{0x02, 0x01, 0x00}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(120, lastTerm)
	requireT.EqualValues(1211, nextIndex)

	file, err := s2.repo.OpenCurrent()
	requireT.NoError(err)
	b := make([]byte, 21)
	_, err = io.ReadFull(file.Reader(), b)
	requireT.NoError(err)
	requireT.Equal([]byte{
		0x9, 0x78, 0x7e, 0xf3, 0x28, 0x61, 0x2b, 0xb6, 0x7c, 0x89,
		0xa, 0x1, 0x0, 0x1, 0xb0, 0x78, 0xc9, 0x3d, 0x1f, 0x3c, 0x9b,
	}, b)
	requireT.NoError(file.Close())
}

func TestNewWithRemainingData(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	}, true, true)
	requireT.NoError(err)

	b := make([]byte, s1.repo.PageCapacity()-8)
	requireT.EqualValues(2, varuint64.Put(b, uint64(len(b)-2)))
	_, _, err = s1.Append(b, false, false)
	requireT.NoError(err)

	s2, _ := newState(t, dir)
	requireT.EqualValues(1, s2.CurrentTerm())
	requireT.EqualValues(1, s2.LastLogTerm())
	requireT.EqualValues(1, s2.highestTermSeen)
	requireT.EqualValues(s2.repo.PageCapacity()+10, s2.NextLogIndex())
	requireT.EqualValues(10, s2.nextLogIndexInFile)
}

func TestNewWithValidData(t *testing.T) {
	requireT := require.New(t)

	s1, _ := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x0a, 0x01, 0x00, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	}, true, true)
	requireT.NoError(err)

	requireT.EqualValues(1, s1.CurrentTerm())
	requireT.EqualValues(1, s1.LastLogTerm())
	requireT.EqualValues(1, s1.highestTermSeen)
	requireT.EqualValues(21, s1.NextLogIndex())
	requireT.EqualValues(21, s1.nextLogIndexInFile)

	file, err := s1.repo.OpenCurrent()
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	requireT.Equal([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x0a, 0x01, 0x00, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	}, data[:21])
	requireT.NoError(file.Close())
}

func TestNewWithInvalidChecksum(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x0a, 0x01, 0x00, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	}, true, true)
	requireT.NoError(err)

	file, err := s1.repo.OpenCurrent()
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	data[20] = 0x00
	requireT.NoError(file.Close())

	s2, _ := newState(t, dir)
	requireT.EqualValues(1, s2.CurrentTerm())
	requireT.EqualValues(1, s2.LastLogTerm())
	requireT.EqualValues(1, s2.highestTermSeen)
	requireT.EqualValues(10, s2.NextLogIndex())
	requireT.EqualValues(10, s2.nextLogIndexInFile)
}

func TestNewWithInvalidSize(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x0a, 0x01, 0x00, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	}, true, true)
	requireT.NoError(err)

	file, err := s1.repo.OpenCurrent()
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	data[10] = 0x80
	data[11] = 0x80
	data[12] = 0x01
	requireT.NoError(file.Close())

	s2, _ := newState(t, dir)
	requireT.EqualValues(1, s2.CurrentTerm())
	requireT.EqualValues(1, s2.LastLogTerm())
	requireT.EqualValues(1, s2.highestTermSeen)
	requireT.EqualValues(10, s2.NextLogIndex())
	requireT.EqualValues(10, s2.nextLogIndexInFile)
}

func TestNewWithZeroSize(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x0a, 0x01, 0x00, 0x64, 0xe7, 0x0, 0x69, 0xd0, 0xe7, 0xe2, 0xe6,
	}, true, true)
	requireT.NoError(err)

	file, err := s1.repo.OpenCurrent()
	requireT.NoError(err)
	data, err := file.Map()
	requireT.NoError(err)
	data[10] = 0x00
	requireT.NoError(file.Close())

	s2, _ := newState(t, dir)
	requireT.EqualValues(1, s2.CurrentTerm())
	requireT.EqualValues(1, s2.LastLogTerm())
	requireT.EqualValues(1, s2.highestTermSeen)
	requireT.EqualValues(10, s2.NextLogIndex())
	requireT.EqualValues(10, s2.nextLogIndexInFile)
}

func TestNewWithFullFile(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	}, true, true)
	requireT.NoError(err)

	b := make([]byte, s1.repo.PageCapacity()-18)
	requireT.EqualValues(2, varuint64.Put(b, uint64(len(b)-2)))
	_, _, err = s1.Append(b, false, false)
	requireT.NoError(err)

	s2, _ := newState(t, dir)
	requireT.EqualValues(1, s2.CurrentTerm())
	requireT.EqualValues(1, s2.LastLogTerm())
	requireT.EqualValues(1, s2.highestTermSeen)
	requireT.EqualValues(s2.repo.PageCapacity(), s2.NextLogIndex())
	requireT.EqualValues(s2.repo.PageCapacity(), s2.nextLogIndexInFile)
}

func TestNewWithBrokenSize(t *testing.T) {
	requireT := require.New(t)

	s1, dir := newState(t, "")

	requireT.NoError(s1.SetCurrentTerm(1))
	_, _, err := s1.Append([]byte{
		0x09, 0x01, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
	}, true, true)
	requireT.NoError(err)

	b := make([]byte, s1.repo.PageCapacity()-19)
	requireT.EqualValues(2, varuint64.Put(b, uint64(len(b)-2)))
	_, _, err = s1.Append(b, false, false)
	requireT.NoError(err)

	requireT.EqualValues(s1.repo.PageCapacity()-1, s1.NextLogIndex())
	requireT.EqualValues(s1.repo.PageCapacity()-1, s1.nextLogIndexInFile)
	s1.log[s1.repo.PageCapacity()-1] = 0x80

	s2, _ := newState(t, dir)
	requireT.EqualValues(1, s2.CurrentTerm())
	requireT.EqualValues(1, s2.LastLogTerm())
	requireT.EqualValues(1, s2.highestTermSeen)
	requireT.EqualValues(s2.repo.PageCapacity()-1, s2.NextLogIndex())
	requireT.EqualValues(s2.repo.PageCapacity()-1, s2.nextLogIndexInFile)
}

func TestSync(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState(t, "")

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append([]byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
	}, false, true)
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(31, nextIndex)
	requireT.EqualValues(31, s.nextLogIndex)
	logEqual(requireT, s,
		0x9, 0x1, 0x8a, 0xa5, 0x40, 0x7e, 0x4a, 0x41, 0x9e, 0x20,
		0x9, 0x2, 0x61, 0x5a, 0x5c, 0xd9, 0x98, 0x56, 0x91, 0x27, 0xa, 0x3, 0x4, 0xa7, 0xfb, 0xbf, 0x97, 0x4f, 0x3a, 0x2b, 0xc6,
	)
	requireT.EqualValues(2, s.LastLogTerm())
	syncedIndex, err := s.Sync()
	requireT.NoError(err)
	requireT.EqualValues(31, syncedIndex)
}
