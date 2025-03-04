package state

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
)

const logSize = 1024 * 1024 * 1024

// CloseFunc defines function type used to close the state.
type CloseFunc func()

// Open opens state existing in directory or creates a new one there.
func Open(dir string, maxReturnedLogSize uint64) (*State, CloseFunc, error) {
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
			log:                log,
			nextLogIndex:       rafttypes.Index(0), // FIXME (wojciech): nextLogIndex must be stored somewhere.
			maxReturnedLogSize: maxReturnedLogSize,
			doMSync:            true,
		}, func() {
			_ = unix.Munmap(log)
			_ = logF.Close()
		}, nil
}

// NewInMemory creates in-memory state useful for testing.
func NewInMemory(logSize, maxReturnedLogSize uint64) *State {
	return &State{
		log:                make([]byte, logSize),
		maxReturnedLogSize: maxReturnedLogSize,
	}
}

// State represents the persistent state of the Raft consensus algorithm.
type State struct {
	currentTerm        rafttypes.Term
	votedFor           types.ServerID
	terms              []rafttypes.Index
	log                []byte
	nextLogIndex       rafttypes.Index
	maxReturnedLogSize uint64
	doMSync            bool
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
	return s.previousTerm(s.nextLogIndex)
}

// NextLogIndex returns the index of the next log entry.
// This is calculated based on the current length of the log.
// It effectively points to the position where a new log entry would be appended.
func (s *State) NextLogIndex() rafttypes.Index {
	return s.nextLogIndex
}

// Entries retrieves the log entries starting from the given nextLogIndex.
// If nextLogIndex is greater than the length of the log, it returns an error indicating a protocol bug.
// For a valid nextLogIndex, it returns the term of the log entry preceding nextLogIndex (or 0 if nextLogIndex is 0),
// the slice of log entries starting at nextLogIndex, and no error.
func (s *State) Entries(nextLogIndex rafttypes.Index) (rafttypes.Term, rafttypes.Term, []byte, error) {
	if nextLogIndex > s.nextLogIndex {
		return 0, 0, nil, errors.New("bug in protocol")
	}

	previousTerm := s.previousTerm(nextLogIndex)
	if nextLogIndex == s.nextLogIndex {
		return previousTerm, previousTerm, nil, nil
	}
	nextTerm := s.previousTerm(nextLogIndex + 1)

	entries := s.log[nextLogIndex:s.nextLogIndex]
	if nextTerm < rafttypes.Term(len(s.terms)) {
		entries = s.log[nextLogIndex:s.terms[nextTerm]]
	}
	if uint64(len(entries)) > s.maxReturnedLogSize {
		entries = entries[:s.maxReturnedLogSize]
	}
	return previousTerm, nextTerm, entries, nil
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
	lastLogTerm, term rafttypes.Term,
	data []byte,
) (rafttypes.Term, rafttypes.Index, error) {
	if term < lastLogTerm {
		return 0, 0, errors.New("bug in protocol")
	}
	if term < 1 {
		return 0, 0, errors.New("bug in protocol")
	}

	if nextLogIndex == 0 {
		if lastLogTerm != 0 {
			return 0, 0, errors.New("bug in protocol")
		}
		return s.appendLog(nextLogIndex, lastLogTerm, term, data)
	}
	if lastLogTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	if nextLogIndex > s.nextLogIndex {
		return s.previousTerm(s.nextLogIndex), s.nextLogIndex, nil
	}

	if s.previousTerm(nextLogIndex) == lastLogTerm {
		return s.appendLog(nextLogIndex, lastLogTerm, term, data)
	}

	revertTerm := s.previousTerm(nextLogIndex) - 1
	s.nextLogIndex = s.terms[revertTerm]
	s.terms = s.terms[:revertTerm]

	return revertTerm, s.nextLogIndex, nil
}

func (s *State) previousTerm(nextIndex rafttypes.Index) rafttypes.Term {
	for i := len(s.terms) - 1; i >= 0; i-- {
		if s.terms[i] < nextIndex {
			return rafttypes.Term(i + 1)
		}
	}
	return 0
}

func (s *State) appendLog(
	nextLogIndex rafttypes.Index,
	lastLogTerm,
	term rafttypes.Term,
	data []byte,
) (rafttypes.Term, rafttypes.Index, error) {
	if s.nextLogIndex > nextLogIndex && term <= s.previousTerm(nextLogIndex+1) {
		return 0, 0, errors.New("bug in protocol")
	}
	s.terms = s.terms[:lastLogTerm]
	s.nextLogIndex = nextLogIndex
	if len(data) > 0 {
		for range term - lastLogTerm {
			s.terms = append(s.terms, nextLogIndex)
		}
		s.nextLogIndex += rafttypes.Index(copy(s.log[nextLogIndex:], data))

		if s.doMSync {
			if err := unix.Msync(s.log, unix.MS_SYNC); err != nil {
				return 0, 0, errors.WithStack(err)
			}
		}
	}
	return rafttypes.Term(len(s.terms)), s.nextLogIndex, nil
}
