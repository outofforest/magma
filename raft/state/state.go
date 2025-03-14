package state

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/varuint64"
)

const logSize = 1024 * 1024 * 1024

// CloseFunc defines function type used to close the state.
type CloseFunc func()

// Open opens state existing in directory or creates a new one there.
func Open(dir string) (*State, CloseFunc, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	logF, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if err := logF.Truncate(logSize); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	log, err := unix.Mmap(int(logF.Fd()), 0, logSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	return &State{
			log:          log,
			nextLogIndex: rafttypes.Index(0),
			doMSync:      true,
		}, func() {
			_ = unix.Munmap(log)
			_ = logF.Close()
		}, nil
}

// NewInMemory creates in-memory state useful for testing.
func NewInMemory(logSize uint64) (*State, []byte) {
	s := &State{
		log: make([]byte, logSize),
	}
	return s, s.log
}

// State represents the persistent state of the Raft consensus algorithm.
type State struct {
	currentTerm  rafttypes.Term
	votedFor     types.ServerID
	terms        []rafttypes.Index
	log          []byte
	nextLogIndex rafttypes.Index

	highestTermSeen rafttypes.Term
	doMSync         bool
}

// CurrentTerm returns the current term of the state.
// The term represents a monotonically increasing number identifying
// the current term in the raft consensus algorithm.
func (s *State) CurrentTerm() rafttypes.Term {
	return s.currentTerm
}

// SetCurrentTerm sets the current term for the state.
// The term provided must be greater than the current term; otherwise, it returns an error,
// since a lower or equal term indicates a protocol inconsistency.
// Setting a new term also resets the votedFor field to ZeroServerID.
func (s *State) SetCurrentTerm(term rafttypes.Term) error {
	if term <= s.currentTerm {
		return errors.New("bug in protocol")
	}
	s.currentTerm = term
	s.votedFor = types.ZeroServerID
	return nil
}

// VoteFor records a vote for a given candidate in the current term.
// It ensures that a vote is only recorded if no vote has been cast yet, or if
// the vote is consistent with the previously cast vote. A vote for the ZeroServerID
// is not allowed, as it would indicate a protocol inconsistency.
// Returns true if the vote was successfully recorded, or false if the vote
// was not recorded due to a prior vote for a different candidate.
func (s *State) VoteFor(candidate types.ServerID) (bool, error) {
	if candidate == types.ZeroServerID {
		return false, errors.New("bug in protocol")
	}
	if s.votedFor != types.ZeroServerID && s.votedFor != candidate {
		return false, nil
	}

	s.votedFor = candidate
	return true, nil
}

// LastLogTerm returns the term of the last log entry in the state.
// If the log is empty, it returns 0.
func (s *State) LastLogTerm() rafttypes.Term {
	return rafttypes.Term(len(s.terms))
}

// NextLogIndex returns the index of the next log entry.
// This is calculated based on the current length of the log.
// It effectively points to the position where a new log entry would be appended.
func (s *State) NextLogIndex() rafttypes.Index {
	return s.nextLogIndex
}

// PreviousTerm returns term of previous log item.
func (s *State) PreviousTerm(index rafttypes.Index) rafttypes.Term {
	for i := len(s.terms) - 1; i >= 0; i-- {
		if s.terms[i] < index {
			return rafttypes.Term(i + 1)
		}
	}
	return 0
}

var termEntry = bytes.Repeat([]byte{0x00}, 2*varuint64.MaxSize)

// AppendTerm appends term to the log.
func (s *State) AppendTerm() (rafttypes.Term, rafttypes.Index, error) {
	n := varuint64.Put(termEntry[varuint64.MaxSize:], uint64(s.currentTerm))
	n2 := varuint64.Size(n)
	varuint64.Put(termEntry[varuint64.MaxSize-n2:], n)
	return s.appendLog(termEntry[varuint64.MaxSize-n2:varuint64.MaxSize+n], true)
}

// Validate validates the common point in log.
func (s *State) Validate(
	nextLogIndex rafttypes.Index,
	lastLogTerm rafttypes.Term,
) (rafttypes.Term, rafttypes.Index, error) {
	if nextLogIndex == 0 {
		return s.validate(nextLogIndex, lastLogTerm)
	}
	if lastLogTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	if nextLogIndex > s.nextLogIndex {
		return s.PreviousTerm(s.nextLogIndex), s.nextLogIndex, nil
	}

	if s.PreviousTerm(nextLogIndex) == lastLogTerm {
		return s.validate(nextLogIndex, lastLogTerm)
	}

	revertTerm := s.PreviousTerm(nextLogIndex) - 1
	s.nextLogIndex = s.terms[revertTerm]
	s.terms = s.terms[:revertTerm]

	return revertTerm, s.nextLogIndex, nil
}

// Append appends data to log.
func (s *State) Append(
	data []byte,
	allowTermMark bool,
) (rafttypes.Term, rafttypes.Index, error) {
	return s.appendLog(data, allowTermMark)
}

// Sync syncs data to persistent storage.
func (s *State) Sync() (rafttypes.Index, error) {
	if s.doMSync {
		if err := unix.Msync(s.log, unix.MS_SYNC); err != nil {
			return 0, errors.WithStack(err)
		}
	}
	return s.nextLogIndex, nil
}

func (s *State) validate(
	nextLogIndex rafttypes.Index,
	lastLogTerm rafttypes.Term,
) (_ rafttypes.Term, _ rafttypes.Index, retErr error) {
	if s.currentTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}
	if nextLogIndex > s.nextLogIndex {
		return 0, 0, errors.New("bug in protocol")
	}
	if nextLogIndex == 0 && lastLogTerm != 0 {
		return 0, 0, errors.New("bug in protocol")
	}
	if nextLogIndex != 0 && lastLogTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}
	if s.PreviousTerm(nextLogIndex) != lastLogTerm {
		return 0, 0, errors.New("bug in protocol")
	}
	if nextLogIndex < s.nextLogIndex && s.PreviousTerm(nextLogIndex+1) == lastLogTerm {
		return 0, 0, errors.New("bug in protocol")
	}

	s.terms = s.terms[:lastLogTerm]
	s.nextLogIndex = nextLogIndex

	return rafttypes.Term(len(s.terms)), s.nextLogIndex, nil
}

func (s *State) appendLog(
	data []byte,
	allowTermMark bool,
) (_ rafttypes.Term, _ rafttypes.Index, retErr error) {
	if s.currentTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	defer appendDefer(&retErr)

	if len(data) > 0 {
		lastLogTerm := rafttypes.Term(len(s.terms))
		d := data
		var i uint64
		for len(d) > 0 {
			size, n1 := varuint64.Parse(d)
			if size == 0 {
				return 0, 0, errors.New("bug in protocol")
			}
			term, n2 := varuint64.Parse(d[n1:])
			switch {
			case n2 == size:
				if !allowTermMark {
					return 0, 0, errors.New("term mark not allowed")
				}

				// This is a term mark.
				if rafttypes.Term(term) <= s.highestTermSeen {
					return 0, 0, errors.New("bug in protocol")
				}
				if rafttypes.Term(term) > s.currentTerm {
					return 0, 0, errors.New("bug in protocol")
				}
				for range rafttypes.Term(term) - lastLogTerm {
					s.terms = append(s.terms, s.nextLogIndex+rafttypes.Index(i))
				}
				lastLogTerm = rafttypes.Term(term)
				s.highestTermSeen = lastLogTerm
			case s.highestTermSeen == 0:
				return 0, 0, errors.New("bug in protocol")
			}
			i += n1 + size
			d = data[i:]
		}
		s.nextLogIndex += rafttypes.Index(copy(s.log[s.nextLogIndex:], data))
	}
	return rafttypes.Term(len(s.terms)), s.nextLogIndex, nil
}

func appendDefer(err *error) {
	if recover() != nil {
		*err = errors.New("invalid transaction")
	}
}
