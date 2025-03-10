package state

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
)

const maxSize = 6

func newState() *State {
	return NewInMemory(1024 * 1024)
}

func appendLog(s *State, data ...byte) {
	copy(s.log[s.nextLogIndex:], data)
	s.nextLogIndex += rafttypes.Index(len(data))
}

func logEqual(requireT *require.Assertions, s *State, expectedLog ...byte) {
	requireT.Equal(rafttypes.Index(len(expectedLog)), s.nextLogIndex)
	requireT.Equal(expectedLog, s.log[:s.nextLogIndex])
}

func TestNewInMemory(t *testing.T) {
	requireT := require.New(t)

	s := NewInMemory(112)
	requireT.False(s.doMSync)
	requireT.Len(s.log, 112)
}

func TestOpen(t *testing.T) {
	requireT := require.New(t)
	dir := t.TempDir()

	s, sClose, err := Open(dir)
	t.Cleanup(sClose)

	requireT.NoError(err)
	requireT.True(s.doMSync)
	requireT.Len(s.log, logSize)
}

func TestCurrentTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.Zero(s.CurrentTerm())

	requireT.NoError(s.SetCurrentTerm(1))
	requireT.EqualValues(1, s.CurrentTerm())

	requireT.NoError(s.SetCurrentTerm(10))
	requireT.EqualValues(10, s.CurrentTerm())

	requireT.Error(s.SetCurrentTerm(10))
	requireT.EqualValues(10, s.CurrentTerm())

	requireT.Error(s.SetCurrentTerm(9))
	requireT.EqualValues(10, s.CurrentTerm())

	requireT.Error(s.SetCurrentTerm(0))
	requireT.EqualValues(10, s.CurrentTerm())
}

func TestSetCurrentTermResetsVotedFor(t *testing.T) {
	requireT := require.New(t)

	s := newState()
	s.votedFor = types.ServerID(uuid.New())

	requireT.NoError(s.SetCurrentTerm(1))
	requireT.EqualValues(types.ZeroServerID, s.votedFor)
}

func TestVoteFor(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	granted, err := s.VoteFor(types.ZeroServerID)
	requireT.Error(err)
	requireT.False(granted)

	candidateID1 := types.ServerID(uuid.New())
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

	requireT.NoError(s.SetCurrentTerm(1))
	granted, err = s.VoteFor(candidateID2)
	requireT.NoError(err)
	requireT.True(granted)

	granted, err = s.VoteFor(candidateID1)
	requireT.NoError(err)
	requireT.False(granted)
}

func TestLastLogTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.EqualValues(0, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 0, 1, 2, 2, 4, 5, 5, 5, 6, 6}
	requireT.EqualValues(11, s.LastLogTerm())

	s.terms = []rafttypes.Index{0}
	requireT.EqualValues(1, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 0, 0}
	requireT.EqualValues(3, s.LastLogTerm())
}

func TestNextLogIndex(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.EqualValues(0, s.NextLogIndex())

	appendLog(s, 0x00)

	requireT.EqualValues(1, s.NextLogIndex())

	appendLog(s, 0x00)

	requireT.EqualValues(2, s.NextLogIndex())
}

func TestPreviousTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.EqualValues(0, s.PreviousTerm(1000))
	requireT.EqualValues(0, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	s.terms = []rafttypes.Index{0}
	requireT.EqualValues(1, s.PreviousTerm(1000))
	requireT.EqualValues(1, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	s.terms = []rafttypes.Index{1}
	requireT.EqualValues(1, s.PreviousTerm(1000))
	requireT.EqualValues(0, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	s.terms = []rafttypes.Index{0, 0, 0}
	requireT.EqualValues(3, s.PreviousTerm(1000))
	requireT.EqualValues(3, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	s.terms = []rafttypes.Index{0, 1, 2}
	requireT.EqualValues(3, s.PreviousTerm(1000))
	requireT.EqualValues(1, s.PreviousTerm(1))
	requireT.EqualValues(0, s.PreviousTerm(0))

	s.terms = []rafttypes.Index{0, 1, 2}
	requireT.EqualValues(3, s.PreviousTerm(1000))
	requireT.EqualValues(2, s.PreviousTerm(2))
	requireT.EqualValues(0, s.PreviousTerm(0))

	s.terms = []rafttypes.Index{0, 0, 3, 3, 3, 5, 5}
	requireT.EqualValues(0, s.PreviousTerm(0))
	requireT.EqualValues(2, s.PreviousTerm(1))
	requireT.EqualValues(2, s.PreviousTerm(2))
	requireT.EqualValues(2, s.PreviousTerm(3))
	requireT.EqualValues(5, s.PreviousTerm(4))
	requireT.EqualValues(5, s.PreviousTerm(5))
	requireT.EqualValues(7, s.PreviousTerm(6))
	requireT.EqualValues(7, s.PreviousTerm(7))
}

func TestEntries(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	lastLogTerm, nextLogTerm, entries, err := s.Entries(1, maxSize)
	requireT.Error(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(0, maxSize)
	requireT.NoError(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	// 0x01 0x01 0x01 0x02 0x01 0x03
	appendLog(s, 0x01, 0x01, 0x01, 0x02, 0x01, 0x03)
	s.terms = []rafttypes.Index{0, 2, 4}

	lastLogTerm, nextLogTerm, entries, err = s.Entries(0, maxSize)
	requireT.NoError(err)
	requireT.Zero(lastLogTerm)
	requireT.EqualValues(1, nextLogTerm)
	requireT.Equal([]byte{0x01, 0x01, 0x01, 0x02, 0x01, 0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(2, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(1, lastLogTerm)
	requireT.EqualValues(2, nextLogTerm)
	requireT.EqualValues([]byte{0x01, 0x02, 0x01, 0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(4, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(2, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.EqualValues([]byte{0x01, 0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(6, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(3, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.Empty(entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(7, maxSize)
	requireT.Error(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	// 0x01 0x01 0x01 0x02 0x01 0x03 0x01 0x04
	appendLog(s, 0x01, 0x04)
	s.terms = []rafttypes.Index{0, 2, 4, 6}

	lastLogTerm, nextLogTerm, entries, err = s.Entries(0, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(0, lastLogTerm)
	requireT.EqualValues(1, nextLogTerm)
	requireT.EqualValues([]byte{0x01, 0x01, 0x01, 0x02, 0x01, 0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(2, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(1, lastLogTerm)
	requireT.EqualValues(2, nextLogTerm)
	requireT.EqualValues([]byte{0x01, 0x02, 0x01, 0x03, 0x01, 0x04}, entries)

	// 0x01 0x01 0x01 0x02 0x01 0x03 0x01 0x04 0x01 0x06 0x02 0x01 0x00 0x02 0x01 0x00
	appendLog(s, 0x01, 0x06, 0x02, 0x01, 0x00, 0x02, 0x01, 0x00)
	s.terms = []rafttypes.Index{0, 2, 4, 6, 8, 8}

	lastLogTerm, nextLogTerm, entries, err = s.Entries(6, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(3, lastLogTerm)
	requireT.EqualValues(4, nextLogTerm)
	requireT.EqualValues([]byte{0x01, 0x04, 0x01, 0x06}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(8, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(4, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.EqualValues([]byte{0x01, 0x06, 0x02, 0x01, 0x00}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(10, maxSize)
	requireT.NoError(err)
	requireT.EqualValues(6, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.EqualValues([]byte{0x02, 0x01, 0x00, 0x02, 0x01, 0x00}, entries)
}

func TestAppendTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(1))
	lastTerm, nextIndex, err := s.AppendTerm()
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
	)

	requireT.NoError(s.SetCurrentTerm(127))
	lastTerm, nextIndex, err = s.AppendTerm()
	requireT.NoError(err)
	requireT.EqualValues(127, lastTerm)
	requireT.EqualValues(4, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x01, 0x7f,
	)

	requireT.NoError(s.SetCurrentTerm(128))
	lastTerm, nextIndex, err = s.AppendTerm()
	requireT.NoError(err)
	requireT.EqualValues(128, lastTerm)
	requireT.EqualValues(7, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x01, 0x7f, 0x02, 0x80, 0x01,
	)
}

func TestAppendErrorIfCurrentTermNotSet(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnInvalidTxSize(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(200))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x80})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Empty(s.terms)
}

func TestAppendErrorIfTxSizeIsTooLow(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfTxSizeIsTooBig1(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x02, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfTxSizeIsTooBig2(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x02, 0x01, 0x00, 0x02, 0x03})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfTxSizeIsZero(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x00})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnInvalidTermNumber(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(200))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x80})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Empty(s.terms)
}

func TestAppendErrorOnNoTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnTermNumberAboveCurrentTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x7f})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Empty(s.terms)
}

func TestAppendErrorOnZeroNextLogIndex(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 1, []byte{0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnZeroLastLogTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(1, 0, []byte{0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorOnNewZeroTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x00})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfSameTermCreatedInSameOperation(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfSameTermCreatedInNextOperation(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 1, []byte{0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendErrorIfLowerTermCreatedInSameOperation(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x02, 0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendErrorIfLowerTermCreatedInNextOperation(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x02})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s,
		0x01, 0x02,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 2, []byte{0x01, 0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendErrorIfOverwrittenWithExistingHigherTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x01, 0x02, 0x01, 0x03})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x01, 0x02, 0x01, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		4,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 1, []byte{0x01, 0x03})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendErrorIfOverwrittenWithExistingSameTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x01, 0x02, 0x01, 0x03})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x01, 0x02, 0x01, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		4,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(4, 2, []byte{0x01, 0x03})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendErrorIfOverwrittenInTheMiddleOfTheTermWithTheSameTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x02, 0x01, 0x00})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(5, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x02, 0x01, 0x00,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 1, []byte{0x02, 0x01, 0x00})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendErrorIfOverwrittenInTheMiddleOfTheTermWithHigherTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x02, 0x01, 0x00})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(5, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x02, 0x01, 0x00,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 1, []byte{0x01, 0x02})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendErrorIfNoTerm(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x02, 0x01, 0x00})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
}

func TestAppendNothingHappensIfPreviousIndexDoesNotExist(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(1, 1, []byte{0x01, 0x01})
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendNilOnEmptyLog(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, nil)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
}

func TestAppendOnEmptyOnlyTerm1(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)
}

func TestAppendOnEmptyOnlyTerm2(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x02})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s,
		0x01, 0x02,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		0,
	}, s.terms)
}

func TestAppendOnEmptyLogWithTerm1(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x01, 0x02, 0x01, 0x00})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(5, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01, 0x02, 0x01, 0x00,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)
}

func TestAppendOnEmptyLogWithTerm2(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{0x01, 0x02, 0x02, 0x01, 0x00})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(5, nextIndex)
	logEqual(requireT, s,
		0x01, 0x02, 0x02, 0x01, 0x00,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		0,
	}, s.terms)
}

func TestAppendHappyPath(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
	})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(7, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(7, 2, []byte{
		0x01, 0x03,
	})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(9, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		7,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(9, 3, nil)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(9, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		7,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 1, []byte{0x01, 0x04, 0x03, 0x00, 0x01, 0x02})
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(8, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x04, 0x03, 0x00, 0x01, 0x02,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		2,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(8, 4, []byte{0x01, 0x06})
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(10, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x04, 0x03, 0x00, 0x01, 0x02,
		0x01, 0x06,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		2,
		2,
		8,
		8,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 1, []byte{0x01, 0x07, 0x02, 0x00, 0x00})
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(7, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x07, 0x02, 0x00, 0x00,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		2,
		2,
		2,
		2,
		2,
	}, s.terms)
}

func TestAppendRevertWhenLastLogDoesNotMatch(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(9, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		7,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(7, 4, nil)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)
}

func TestAppendRevertToNothing(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	requireT.NoError(s.SetCurrentTerm(100))

	lastTerm, nextIndex, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(9, nextIndex)
	logEqual(requireT, s,
		0x01, 0x01,
		0x01, 0x02, 0x02, 0x03, 0x04,
		0x01, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		2,
		7,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 4, nil)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Empty(s.terms)
}
