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
	pageSize := os.Getpagesize()
	size := (logSize + pageSize - 1) / pageSize * pageSize
	if err := logF.Truncate(int64(size)); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	log, err := unix.Mmap(int(logF.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
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
func NewInMemory(logSize uint64) *State {
	return &State{
		log: make([]byte, logSize),
	}
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

// Entries retrieves the log entries starting from the given nextLogIndex.
// If nextLogIndex is greater than the length of the log, it returns an error indicating a protocol bug.
// For a valid nextLogIndex, it returns the term of the log entry preceding nextLogIndex (or 0 if nextLogIndex is 0),
// the slice of log entries starting at nextLogIndex, and no error.
func (s *State) Entries(startIndex rafttypes.Index, maxSize uint64) (rafttypes.Term, rafttypes.Term, []byte, error) {
	if startIndex > s.nextLogIndex {
		return 0, 0, nil, errors.New("bug in protocol")
	}

	previousTerm := s.PreviousTerm(startIndex)
	if startIndex == s.nextLogIndex {
		return previousTerm, previousTerm, nil, nil
	}

	entries := s.log[startIndex:s.nextLogIndex]
	if uint64(len(entries)) > maxSize {
		var i uint64
		for i < maxSize {
			size, n := varuint64.Parse(entries[i:])
			if i+n+size > maxSize {
				break
			}
			i += n + size
		}
		entries = entries[:i]
	}

	return previousTerm, s.PreviousTerm(startIndex + 1), entries, nil
}

var termEntry = bytes.Repeat([]byte{0x00}, 2*varuint64.MaxSize)

// AppendTerm appends term to the log.
func (s *State) AppendTerm() (rafttypes.Term, rafttypes.Index, error) {
	n := varuint64.Put(termEntry[varuint64.MaxSize:], uint64(s.currentTerm))
	n2 := varuint64.Size(n)
	varuint64.Put(termEntry[varuint64.MaxSize-n2:], n)
	return s.appendLog(s.nextLogIndex, s.LastLogTerm(), termEntry[varuint64.MaxSize-n2:varuint64.MaxSize+n], true)
}

// Append attempts to apply the given log entries starting at a specified index in the log.
// It verifies that the provided `nextLogIndex` and `lastLogTerm` are consistent with the
// existing log. If they are not consistent, it either truncates conflicting entries or
// returns an error depending on the situation.
//
// Parameters:
//   - nextLogIndex: The expected starting index for the given entries in the log.
//   - lastLogTerm: The term of the log entry immediately preceding `nextLogIndex`.
//     If this term does not match the corresponding term in the log, it indicates
//     an inconsistency.
//   - entries: A slice of log entries to append to the state log.
//
// Returns:
// - types.Term: The term of the last log entry after appending (if successful).
// - types.Index: The index of the last log entry after appending (if successful).
// - bool: A flag indicating whether the log was successfully updated.
// - error: An error indicating a protocol inconsistency or other issues during processing.
//
// Behavior:
//   - If nextLogIndex is 0, the log is fully replaced with the new entries, with
//     specific checks on term consistency.
//   - If the term consistency is validated, the new entries are appended, potentially
//     overwriting conflicting existing entries starting from `nextLogIndex`.
//   - If term inconsistency is detected, conflicting entries are truncated,
//     and the function exits without appending the new entries.
//
// The function will ensure that no log entry is appended out of order or violates
// the consistency guarantees of the Raft protocol.
func (s *State) Append(
	nextLogIndex rafttypes.Index,
	lastLogTerm rafttypes.Term,
	data []byte,
	allowTermMark bool,
) (rafttypes.Term, rafttypes.Index, error) {
	if nextLogIndex == 0 {
		return s.appendLog(nextLogIndex, lastLogTerm, data, allowTermMark)
	}
	if lastLogTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	if nextLogIndex > s.nextLogIndex {
		return s.PreviousTerm(s.nextLogIndex), s.nextLogIndex, nil
	}

	if s.PreviousTerm(nextLogIndex) == lastLogTerm {
		return s.appendLog(nextLogIndex, lastLogTerm, data, allowTermMark)
	}

	revertTerm := s.PreviousTerm(nextLogIndex) - 1
	s.nextLogIndex = s.terms[revertTerm]
	s.terms = s.terms[:revertTerm]

	return revertTerm, s.nextLogIndex, nil
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

func (s *State) appendLog(
	nextLogIndex rafttypes.Index,
	lastLogTerm rafttypes.Term,
	data []byte,
	allowTermMark bool,
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

	defer appendDefer(&retErr)

	s.terms = s.terms[:lastLogTerm]
	s.nextLogIndex = nextLogIndex
	if len(data) > 0 {
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
					s.terms = append(s.terms, nextLogIndex+rafttypes.Index(i))
				}
				lastLogTerm = rafttypes.Term(term)
				s.highestTermSeen = lastLogTerm
			case s.highestTermSeen == 0:
				return 0, 0, errors.New("bug in protocol")
			}
			i += n1 + size
			d = data[i:]
		}
		s.nextLogIndex += rafttypes.Index(copy(s.log[nextLogIndex:], data))
	}
	return rafttypes.Term(len(s.terms)), s.nextLogIndex, nil
}

func appendDefer(err *error) {
	if recover() != nil {
		*err = errors.New("invalid transaction")
	}
}
