package state

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
)

func newState() *State {
	return NewInMemory(1024*1024, 3)
}

func setLog(s *State, log ...byte) {
	copy(s.log, log)
	s.nextLogIndex = rafttypes.Index(len(log))
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

	s := NewInMemory(112, 128*1024)
	requireT.False(s.doMSync)
	requireT.Len(s.log, 112)
	requireT.EqualValues(128*1024, s.maxReturnedLogSize)
}

func TestOpen(t *testing.T) {
	requireT := require.New(t)
	dir := t.TempDir()

	s, sClose, err := Open(dir, 128*1024)
	t.Cleanup(sClose)

	requireT.NoError(err)
	requireT.True(s.doMSync)
	requireT.Len(s.log, logSize)
	requireT.EqualValues(128*1024, s.maxReturnedLogSize)
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
	requireT.EqualValues(0, s.LastLogTerm())

	setLog(s, 0x00)

	s.terms = []rafttypes.Index{0}
	requireT.EqualValues(1, s.LastLogTerm())
	s.terms = []rafttypes.Index{0, 0, 0}
	requireT.EqualValues(3, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 0, 1}
	requireT.EqualValues(2, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 0, 1, 1}
	requireT.EqualValues(2, s.LastLogTerm())
	s.terms = []rafttypes.Index{0, 0, 1, 2}
	requireT.EqualValues(2, s.LastLogTerm())

	setLog(s, 0x00, 0x00, 0x00)

	s.terms = []rafttypes.Index{}
	requireT.EqualValues(0, s.LastLogTerm())

	s.terms = []rafttypes.Index{0}
	requireT.EqualValues(1, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 0, 0}
	requireT.EqualValues(3, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1}
	requireT.EqualValues(2, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1, 1}
	requireT.EqualValues(3, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 2}
	requireT.EqualValues(2, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 2, 2}
	requireT.EqualValues(3, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 2, 3}
	requireT.EqualValues(2, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1, 2, 3}
	requireT.EqualValues(3, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1, 2, 2, 3}
	requireT.EqualValues(4, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 10, 12, 12, 13}
	requireT.EqualValues(1, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1, 10, 12, 12, 13}
	requireT.EqualValues(2, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 3, 10, 12, 12, 13}
	requireT.EqualValues(1, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1, 1, 10, 12, 12, 13}
	requireT.EqualValues(3, s.LastLogTerm())

	s.terms = []rafttypes.Index{0, 1, 1, 2, 2, 10, 12, 12, 13}
	requireT.EqualValues(5, s.LastLogTerm())
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

func TestEntries(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	lastLogTerm, nextLogTerm, entries, err := s.Entries(1)
	requireT.Error(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(0)
	requireT.NoError(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	// 0x01 0x02 0x03
	appendLog(s, 0x01, 0x02, 0x03)
	s.terms = []rafttypes.Index{0, 1, 2}

	lastLogTerm, nextLogTerm, entries, err = s.Entries(0)
	requireT.NoError(err)
	requireT.Zero(lastLogTerm)
	requireT.EqualValues(1, nextLogTerm)
	requireT.Equal([]byte{0x01}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues(1, lastLogTerm)
	requireT.EqualValues(2, nextLogTerm)
	requireT.EqualValues([]byte{0x02}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(2)
	requireT.NoError(err)
	requireT.EqualValues(2, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.EqualValues([]byte{0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.Empty(entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(4)
	requireT.Error(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	// 0x01 0x02 0x03 0x03 0x03 0x04 0x04
	appendLog(s, 0x03, 0x03, 0x04, 0x04)
	s.terms = []rafttypes.Index{0, 1, 2, 5, 5}

	lastLogTerm, nextLogTerm, entries, err = s.Entries(2)
	requireT.NoError(err)
	requireT.EqualValues(2, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.EqualValues([]byte{0x03, 0x03, 0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues(3, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.EqualValues([]byte{0x03, 0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(4)
	requireT.NoError(err)
	requireT.EqualValues(3, lastLogTerm)
	requireT.EqualValues(3, nextLogTerm)
	requireT.EqualValues([]byte{0x03}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(5)
	requireT.NoError(err)
	requireT.EqualValues(3, lastLogTerm)
	requireT.EqualValues(5, nextLogTerm)
	requireT.EqualValues([]byte{0x04, 0x04}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(6)
	requireT.NoError(err)
	requireT.EqualValues(5, lastLogTerm)
	requireT.EqualValues(5, nextLogTerm)
	requireT.EqualValues([]byte{0x04}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(7)
	requireT.NoError(err)
	requireT.EqualValues(5, lastLogTerm)
	requireT.EqualValues(5, nextLogTerm)
	requireT.Empty(entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(8)
	requireT.Error(err)
	requireT.Zero(lastLogTerm)
	requireT.Zero(nextLogTerm)
	requireT.Empty(entries)

	// 0x01 0x02 0x03 0x03 0x03 0x04 0x04, 0x05, 0x05, 0x05, 0x05, 0x05
	appendLog(s, 0x05, 0x05, 0x05, 0x05, 0x05)
	s.terms = []rafttypes.Index{0, 1, 2, 5, 5, 7}

	lastLogTerm, nextLogTerm, entries, err = s.Entries(7)
	requireT.NoError(err)
	requireT.EqualValues(5, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.Equal([]byte{0x05, 0x05, 0x05}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(8)
	requireT.NoError(err)
	requireT.EqualValues(6, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.Equal([]byte{0x05, 0x05, 0x05}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(9)
	requireT.NoError(err)
	requireT.EqualValues(6, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.Equal([]byte{0x05, 0x05, 0x05}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(10)
	requireT.NoError(err)
	requireT.EqualValues(6, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.Equal([]byte{0x05, 0x05}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(11)
	requireT.NoError(err)
	requireT.EqualValues(6, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.Equal([]byte{0x05}, entries)

	lastLogTerm, nextLogTerm, entries, err = s.Entries(12)
	requireT.NoError(err)
	requireT.EqualValues(6, lastLogTerm)
	requireT.EqualValues(6, nextLogTerm)
	requireT.Empty(entries)
}

func TestAppend(t *testing.T) {
	requireT := require.New(t)

	s := newState()

	lastTerm, nextIndex, err := s.Append(0, 1, 1, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)

	lastTerm, nextIndex, err = s.Append(1, 0, 1, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)

	lastTerm, nextIndex, err = s.Append(0, 0, 0, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)

	lastTerm, nextIndex, err = s.Append(1, 1, 0, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)

	lastTerm, nextIndex, err = s.Append(1, 1, 1, []byte{0x01})
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)

	lastTerm, nextIndex, err = s.Append(0, 0, 1, nil)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)

	lastTerm, nextIndex, err = s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(1, nextIndex)
	logEqual(requireT, s, 0x01)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(0, 0, 1, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s, 0x01)
	requireT.EqualValues([]rafttypes.Index{
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(0, 0, 2, []byte{0x02})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(1, nextIndex)
	logEqual(requireT, s, 0x02)
	requireT.EqualValues([]rafttypes.Index{
		0,
		0,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(0, 0, 1, []byte{0x01})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s, 0x02)
	requireT.EqualValues([]rafttypes.Index{
		0,
		0,
	}, s.terms)

	setLog(s, 0x01)
	s.terms = []rafttypes.Index{
		0,
	}

	lastTerm, nextIndex, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(2, nextIndex)
	logEqual(requireT, s, 0x01, 0x02)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 3, 3, nil)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(4, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03, 0x04)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		3,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 3, 5, []byte{0x05})
	requireT.NoError(err)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(4, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03, 0x05)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		3,
		3,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 3, 3, nil)
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03, 0x05)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		3,
		3,
	}, s.terms)

	setLog(s, 0x01, 0x02, 0x03)
	s.terms = []rafttypes.Index{
		0,
		1,
		2,
	}

	lastTerm, nextIndex, err = s.Append(4, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(3, 3, 3, []byte{0x03})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(4, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03, 0x03)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(4, 3, 3, []byte{0x03, 0x03})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	logEqual(requireT, s, 0x01, 0x02, 0x03, 0x03, 0x03, 0x03)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(6, 3, 4, []byte{0x04, 0x04, 0x04})
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(9, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04, 0x04, 0x04,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(9, 4, 5, []byte{0x05})
	requireT.NoError(err)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(10, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04, 0x04, 0x04,
		0x05,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
		9,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(10, 5, 6, []byte{0x06})
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(11, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04, 0x04, 0x04,
		0x05,
		0x06,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
		9,
		10,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(11, 7, 7, []byte{0x07})
	requireT.NoError(err)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(10, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04, 0x04, 0x04,
		0x05,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
		9,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(9, 4, 4, []byte{0x04})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04, 0x04, 0x04,
		0x05,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
		9,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(9, 4, 5, []byte{0x05})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04, 0x04, 0x04,
		0x05,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
		9,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(8, 5, 6, []byte{0x06})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(6, 3, 4, nil)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(6, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(7, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
		0x04,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
		6,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(6, 3, 5, nil)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(6, 3, 2, []byte{0x04})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	logEqual(requireT, s,
		0x01,
		0x02,
		0x03, 0x03, 0x03, 0x03,
	)
	requireT.EqualValues([]rafttypes.Index{
		0,
		1,
		2,
	}, s.terms)

	lastTerm, nextIndex, err = s.Append(1, 2, 3, []byte{0x03})
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.Zero(s.nextLogIndex)
	requireT.Empty(s.terms)
}
