package events

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/events/codec"
	"github.com/outofforest/magma/state/events/format"
	magmatypes "github.com/outofforest/magma/types"
)

func newManager(t *testing.T, dir string) (*Store, string) {
	if dir == "" {
		dir = t.TempDir()
	}
	m, err := Open(dir)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, m.Close())
	})

	return m, dir
}

func TestEmptyState(t *testing.T) {
	requireT := require.New(t)
	m, _ := newManager(t, "")

	requireT.Equal(State{}, m.State())
}

func TestTermSet(t *testing.T) {
	requireT := require.New(t)
	m, dir := newManager(t, "")

	s, err := m.Term(1)
	requireT.NoError(err)
	requireT.Equal(State{
		Term: 1,
	}, s)
	requireT.Equal(s, m.State())

	s, err = m.Term(2)
	requireT.NoError(err)
	requireT.Equal(State{
		Term: 2,
	}, s)
	requireT.Equal(s, m.State())

	f, err := os.Open(filepath.Join(dir, "0"))
	requireT.NoError(err)
	defer f.Close()
	d := codec.NewDecoder(f, format.NewMarshaller())

	_, v, err := d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 1}, v)

	_, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 2}, v)

	_, v, err = d.Decode()
	requireT.ErrorIs(err, io.EOF)
	requireT.Nil(v)
}

func TestTermTwoFiles(t *testing.T) {
	requireT := require.New(t)
	m, dir := newManager(t, "")

	for i := range termsPerFile + 1 {
		s, err := m.Term(types.Term(i + 1))
		requireT.NoError(err)
		requireT.Equal(State{
			Term: types.Term(i + 1),
		}, s)
		requireT.Equal(s, m.State())
	}

	f0, err := os.Open(filepath.Join(dir, "0"))
	requireT.NoError(err)
	defer f0.Close()
	d := codec.NewDecoder(f0, format.NewMarshaller())

	for i := range termsPerFile - 1 {
		_, v, err := d.Decode()
		requireT.NoError(err, i)
		requireT.Equal(&format.Term{Term: types.Term(i + 1)}, v)
	}

	_, v, err := d.Decode()
	requireT.ErrorIs(err, io.EOF)
	requireT.Nil(v)

	f1, err := os.Open(filepath.Join(dir, "1"))
	requireT.NoError(err)
	defer f1.Close()
	d = codec.NewDecoder(f1, format.NewMarshaller())

	_, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: termsPerFile}, v)

	_, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: termsPerFile + 1}, v)

	_, v, err = d.Decode()
	requireT.ErrorIs(err, io.EOF)
	requireT.Nil(v)
}

func TestTermFailsIfZero(t *testing.T) {
	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(0)
	requireT.Error(err)
}

func TestTermFailsIfLower(t *testing.T) {
	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(2)
	requireT.NoError(err)

	_, err = m.Term(1)
	requireT.Error(err)
}

func TestTermFailsIfSame(t *testing.T) {
	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(2)
	requireT.NoError(err)

	_, err = m.Term(2)
	requireT.Error(err)
}

func TestVoteSet(t *testing.T) {
	candidate1 := magmatypes.ServerID(uuid.New())
	candidate2 := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m, dir := newManager(t, "")

	_, err := m.Term(1)
	requireT.NoError(err)

	s, err := m.Vote(candidate1)
	requireT.NoError(err)
	requireT.Equal(State{
		Term:     1,
		VotedFor: candidate1,
	}, s)
	requireT.Equal(s, m.State())

	_, err = m.Term(2)
	requireT.NoError(err)

	s, err = m.Vote(candidate2)
	requireT.NoError(err)
	requireT.Equal(State{
		Term:     2,
		VotedFor: candidate2,
	}, s)
	requireT.Equal(s, m.State())

	f, err := os.Open(filepath.Join(dir, "0"))
	requireT.NoError(err)
	defer f.Close()
	d := codec.NewDecoder(f, format.NewMarshaller())

	_, v, err := d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 1}, v)

	_, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Vote{Candidate: candidate1}, v)

	_, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 2}, v)

	_, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Vote{Candidate: candidate2}, v)

	_, v, err = d.Decode()
	requireT.ErrorIs(err, io.EOF)
	requireT.Nil(v)
}

func TestVoteIsZeroedOnNewTerm(t *testing.T) {
	candidate := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(1)
	requireT.NoError(err)

	_, err = m.Vote(candidate)
	requireT.NoError(err)

	s, err := m.Term(2)
	requireT.NoError(err)
	requireT.Equal(State{
		Term: 2,
	}, s)
	requireT.Equal(s, m.State())
}

func TestVoteFailsIfNoTerm(t *testing.T) {
	candidate := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Vote(candidate)
	requireT.Error(err)
}

func TestVoteFailsIfCandidateIsInvalid(t *testing.T) {
	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(1)
	requireT.NoError(err)

	_, err = m.Vote(magmatypes.ZeroServerID)
	requireT.Error(err)
}

func TestVoteFailsIfVotedTwiceOnDifferentCandidates(t *testing.T) {
	candidate1 := magmatypes.ServerID(uuid.New())
	candidate2 := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(1)
	requireT.NoError(err)

	_, err = m.Vote(candidate1)
	requireT.NoError(err)

	_, err = m.Vote(candidate2)
	requireT.Error(err)
}

func TestVoteFailsIfVotedTwiceOnSameCandidate(t *testing.T) {
	candidate := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m, _ := newManager(t, "")

	_, err := m.Term(1)
	requireT.NoError(err)

	_, err = m.Vote(candidate)
	requireT.NoError(err)

	_, err = m.Vote(candidate)
	requireT.Error(err)
}

func TestOpenWithTerm(t *testing.T) {
	requireT := require.New(t)
	m1, dir := newManager(t, "")

	_, err := m1.Term(1)
	requireT.NoError(err)

	_, err = m1.Term(2)
	requireT.NoError(err)

	m2, _ := newManager(t, dir)
	requireT.Equal(State{
		Term: 2,
	}, m2.State())
}

func TestOpenWithTermAndVote(t *testing.T) {
	candidate1 := magmatypes.ServerID(uuid.New())
	candidate2 := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m1, dir := newManager(t, "")

	_, err := m1.Term(1)
	requireT.NoError(err)

	_, err = m1.Vote(candidate1)
	requireT.NoError(err)

	_, err = m1.Term(2)
	requireT.NoError(err)

	_, err = m1.Vote(candidate2)
	requireT.NoError(err)

	m2, _ := newManager(t, dir)
	requireT.Equal(State{
		Term:     2,
		VotedFor: candidate2,
	}, m2.State())
}

func TestOpenWithTermAndZeroedVote(t *testing.T) {
	candidate := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m1, dir := newManager(t, "")

	_, err := m1.Term(1)
	requireT.NoError(err)

	_, err = m1.Vote(candidate)
	requireT.NoError(err)

	_, err = m1.Term(2)
	requireT.NoError(err)

	m2, _ := newManager(t, dir)
	requireT.Equal(State{
		Term: 2,
	}, m2.State())
}

func TestOpenWithManyFiles(t *testing.T) {
	candidate := magmatypes.ServerID(uuid.New())

	requireT := require.New(t)
	m1, dir := newManager(t, "")

	for i := range termsPerFile {
		_, err := m1.Term(types.Term(i + 1))
		requireT.NoError(err)
	}

	_, err := m1.Vote(candidate)
	requireT.NoError(err)

	m2, _ := newManager(t, dir)
	requireT.Equal(State{
		Term:     termsPerFile,
		VotedFor: candidate,
	}, m2.State())
}
