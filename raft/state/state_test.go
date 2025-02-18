package state

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
)

func TestCurrentItem(t *testing.T) {
	requireT := require.New(t)

	s := &State{}

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

func TestVoteFor(t *testing.T) {
	requireT := require.New(t)

	s := &State{}

	granted, err := s.VoteFor(types.ServerID{})
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

	s := &State{}

	requireT.EqualValues(0, s.LastLogTerm())

	s.log = append(s.log, LogItem{Term: 1})

	requireT.EqualValues(1, s.LastLogTerm())

	s.log = append(s.log, LogItem{Term: 2})

	requireT.EqualValues(2, s.LastLogTerm())
}

func TestNextLogIndex(t *testing.T) {
	requireT := require.New(t)

	s := &State{}

	requireT.EqualValues(0, s.NextLogIndex())

	s.log = append(s.log, LogItem{Term: 1})

	requireT.EqualValues(1, s.NextLogIndex())

	s.log = append(s.log, LogItem{Term: 1})

	requireT.EqualValues(2, s.NextLogIndex())
}

func TestEntries(t *testing.T) {
	requireT := require.New(t)

	s := &State{}

	term, entries, err := s.Entries(1)
	requireT.Error(err)
	requireT.Zero(term)
	requireT.Empty(entries)

	term, entries, err = s.Entries(0)
	requireT.NoError(err)
	requireT.Zero(term)
	requireT.Empty(entries)

	s.log = append(s.log,
		LogItem{Term: 1},
		LogItem{Term: 2},
		LogItem{Term: 3},
	)

	term, entries, err = s.Entries(0)
	requireT.NoError(err)
	requireT.Zero(term)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
	}, entries)

	term, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues(1, term)
	requireT.EqualValues([]LogItem{
		{Term: 2},
		{Term: 3},
	}, entries)

	term, entries, err = s.Entries(2)
	requireT.NoError(err)
	requireT.EqualValues(2, term)
	requireT.EqualValues([]LogItem{
		{Term: 3},
	}, entries)

	term, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues(3, term)
	requireT.Empty(entries)

	term, entries, err = s.Entries(4)
	requireT.Error(err)
	requireT.Zero(term)
	requireT.Empty(entries)
}

func TestAppend(t *testing.T) {
	requireT := require.New(t)

	s := &State{}

	lastTerm, nextIndex, success, err := s.Append(0, 1, []LogItem{{Term: 1}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.Empty(s.log)

	lastTerm, nextIndex, success, err = s.Append(1, 0, []LogItem{{Term: 1}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.Empty(s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, []LogItem{
		{Term: 1},
		{Term: 5},
		{Term: 2},
		{Term: 10},
	})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.Empty(s.log)

	lastTerm, nextIndex, success, err = s.Append(1, 1, []LogItem{{Term: 1}})
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.Empty(s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, nil)
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.True(success)
	requireT.Empty(s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, []LogItem{{Term: 1}})
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(1, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, []LogItem{{Term: 1}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, []LogItem{
		{Term: 2},
		{Term: 4},
		{Term: 3},
	})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, []LogItem{
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.EqualValues(2, lastTerm)
	requireT.EqualValues(1, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 2},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(0, 0, []LogItem{{Term: 1}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 2},
	}, s.log)

	s.log = []LogItem{{Term: 1}}

	lastTerm, nextIndex, success, err = s.Append(1, 1, []LogItem{
		{Term: 2},
		{Term: 3},
	})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(3, 3, nil)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(2, 2, []LogItem{{Term: 3}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(3, 3, []LogItem{{Term: 4}})
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(4, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(3, 3, []LogItem{{Term: 5}})
	requireT.NoError(err)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(4, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 5},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(3, 3, nil)
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(4, 3, []LogItem{{Term: 4}})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(3, nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(3, 3, []LogItem{{Term: 3}})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(4, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(3, 3, []LogItem{
		{Term: 4},
		{Term: 5},
		{Term: 4},
		{Term: 6},
	})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(4, 3, []LogItem{
		{Term: 3},
		{Term: 3},
		{Term: 4},
		{Term: 4},
		{Term: 4},
		{Term: 5},
		{Term: 6},
	})
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(11, nextIndex)
	requireT.True(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 4},
		{Term: 4},
		{Term: 4},
		{Term: 5},
		{Term: 6},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(11, 7, []LogItem{{Term: 7}})
	requireT.NoError(err)
	requireT.EqualValues(5, lastTerm)
	requireT.EqualValues(10, nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 4},
		{Term: 4},
		{Term: 4},
		{Term: 5},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(9, 4, []LogItem{{Term: 4}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 4},
		{Term: 4},
		{Term: 4},
		{Term: 5},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(9, 4, []LogItem{{Term: 5}})
	requireT.Error(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 4},
		{Term: 4},
		{Term: 4},
		{Term: 5},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(8, 5, []LogItem{{Term: 6}})
	requireT.NoError(err)
	requireT.EqualValues(3, lastTerm)
	requireT.EqualValues(6, nextIndex)
	requireT.False(success)
	requireT.EqualValues([]LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 3},
		{Term: 3},
	}, s.log)

	lastTerm, nextIndex, success, err = s.Append(1, 2, []LogItem{{Term: 3}})
	requireT.NoError(err)
	requireT.Zero(lastTerm)
	requireT.Zero(nextIndex)
	requireT.False(success)
	requireT.Empty(s.log)
}
