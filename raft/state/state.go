package state

import (
	"github.com/pkg/errors"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
)

// State represents the persistent state of the Raft consensus algorithm.
type State struct {
	currentTerm rafttypes.Term
	votedFor    types.ServerID
	terms       []rafttypes.Index
	log         []byte
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
	return s.previousTerm(rafttypes.Index(len(s.log)))
}

// NextLogIndex returns the index of the next log entry.
// This is calculated based on the current length of the log.
// It effectively points to the position where a new log entry would be appended.
func (s *State) NextLogIndex() rafttypes.Index {
	return rafttypes.Index(len(s.log))
}

// Entries retrieves the log entries starting from the given nextLogIndex.
// If nextLogIndex is greater than the length of the log, it returns an error indicating a protocol bug.
// For a valid nextLogIndex, it returns the term of the log entry preceding nextLogIndex (or 0 if nextLogIndex is 0),
// the slice of log entries starting at nextLogIndex, and no error.
func (s *State) Entries(nextLogIndex rafttypes.Index) (rafttypes.Term, rafttypes.Term, []byte, error) {
	if nextLogIndex > rafttypes.Index(len(s.log)) {
		return 0, 0, nil, errors.New("bug in protocol")
	}

	previousTerm := s.previousTerm(nextLogIndex)
	if nextLogIndex == rafttypes.Index(len(s.log)) {
		return previousTerm, previousTerm, nil, nil
	}
	nextTerm := s.previousTerm(nextLogIndex + 1)
	if nextTerm < rafttypes.Term(len(s.terms)) {
		return previousTerm, nextTerm, s.log[nextLogIndex:s.terms[nextTerm]], nil
	}
	return previousTerm, nextTerm, s.log[nextLogIndex:], nil
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
	term rafttypes.Term,
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
		if rafttypes.Index(len(s.log)) > 0 && term <= s.previousTerm(1) {
			return 0, 0, errors.New("bug in protocol")
		}
		s.log = data
		s.terms = s.terms[:0]
		if len(s.log) == 0 {
			return 0, 0, nil
		}
		for range term {
			s.terms = append(s.terms, 0)
		}
		return term, rafttypes.Index(len(s.log)), nil
	}
	if lastLogTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	if nextLogIndex > rafttypes.Index(len(s.log)) {
		if len(s.log) == 0 {
			return 0, 0, nil
		}
		return s.previousTerm(rafttypes.Index(len(s.log))), rafttypes.Index(len(s.log)), nil
	}

	if s.previousTerm(nextLogIndex) == lastLogTerm {
		if rafttypes.Index(len(s.log)) > nextLogIndex && term <= s.previousTerm(nextLogIndex+1) {
			return 0, 0, errors.New("bug in protocol")
		}
		s.log = append(s.log[:nextLogIndex], data...)
		s.terms = s.terms[:lastLogTerm]
		for range term - lastLogTerm {
			s.terms = append(s.terms, nextLogIndex)
		}
		return term, rafttypes.Index(len(s.log)), nil
	}

	revertTerm := s.previousTerm(nextLogIndex)
	revertIndex := s.terms[revertTerm-1]
	s.log = s.log[:revertIndex]
	s.terms = s.terms[:revertTerm-1]

	return revertTerm - 1, revertIndex, nil
}

func (s *State) previousTerm(nextIndex rafttypes.Index) rafttypes.Term {
	for i := len(s.terms) - 1; i >= 0; i-- {
		if s.terms[i] < nextIndex {
			return rafttypes.Term(i + 1)
		}
	}
	return 0
}
