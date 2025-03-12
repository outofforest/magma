package state

import (
	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/state/format"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/varuint64"
)

type Closer func() error

// Open opens state existing in directory or creates a new one there.
func Open(repo *Repository) (*State, Closer, error) {
	currentFile, txOffset, err := repo.OpenCurrent()
	if err != nil {
		return nil, nil, err
	}

	var nextLogIndex rafttypes.Index
	var log []byte
	if currentFile != nil {
		var err error
		log, err = currentFile.Map()
		if err != nil {
			_ = currentFile.Close()
			return nil, nil, err
		}
		nextLogIndex = findNextIndex(log, txOffset)
	}

	s := &State{
		repo:         repo,
		nextLogIndex: nextLogIndex,
		currentFile:  currentFile,
		log:          log,
	}
	return s, s.close, nil
}

// State represents the persistent state of the Raft consensus algorithm.
type State struct {
	repo         *Repository
	nextLogIndex rafttypes.Index

	currentTerm        rafttypes.Term
	votedFor           types.ServerID
	currentFile        *File
	log                []byte
	nextLogIndexInFile rafttypes.Index

	highestTermSeen rafttypes.Term
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
	return s.repo.LastTerm()
}

// NextLogIndex returns the index of the next log entry.
// This is calculated based on the current length of the log.
// It effectively points to the position where a new log entry would be appended.
func (s *State) NextLogIndex() rafttypes.Index {
	return s.nextLogIndex
}

// PreviousTerm returns term of previous log item.
func (s *State) PreviousTerm(index rafttypes.Index) rafttypes.Term {
	return s.repo.PreviousTerm(index)
}

// AppendTerm appends term to the log.
func (s *State) AppendTerm() (rafttypes.Term, rafttypes.Index, error) {
	var termEntry [2*varuint64.MaxSize + format.ChecksumSize]byte

	n := varuint64.Put(termEntry[varuint64.MaxSize:], uint64(s.currentTerm))
	n2 := varuint64.Size(n + format.ChecksumSize)
	varuint64.Put(termEntry[varuint64.MaxSize-n2:], n+format.ChecksumSize)

	entry := termEntry[varuint64.MaxSize-n2 : varuint64.MaxSize+n+format.ChecksumSize]
	format.PutChecksum(entry[n2:])
	return s.appendLog(entry, true)
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

	return s.repo.Revert(s.PreviousTerm(nextLogIndex) - 1)
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
	if s.currentFile != nil {
		if err := s.currentFile.Sync(); err != nil {
			return 0, err
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
	if nextLogIndex < s.nextLogIndex {
		if s.PreviousTerm(nextLogIndex+1) == lastLogTerm {
			return 0, 0, errors.New("bug in protocol")
		}
		if _, _, err := s.repo.Revert(lastLogTerm); err != nil {
			return 0, 0, err
		}
	}

	s.nextLogIndex = nextLogIndex

	return lastLogTerm, s.nextLogIndex, nil
}

func (s *State) appendLog(
	data []byte,
	allowTermMark bool,
) (_ rafttypes.Term, _ rafttypes.Index, retErr error) {
	if s.currentTerm == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	defer appendDefer(&retErr)

	for len(data) > 0 {
		size, n1 := varuint64.Parse(data)
		if size == 0 {
			return 0, 0, errors.New("bug in protocol")
		}
		if !format.VerifyChecksum(data[n1 : n1+size]) {
			return 0, 0, errors.New("tx checksum mismatch")
		}

		term, n2 := varuint64.Parse(data[n1:])
		switch {
		case n2+format.ChecksumSize == size:
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

			var err error
			s.currentFile, err = s.repo.Create(rafttypes.Term(term), s.nextLogIndex, nil)
			if err != nil {
				return 0, 0, err
			}
			s.log, err = s.currentFile.Map()
			if err != nil {
				return 0, 0, err
			}
			s.nextLogIndexInFile = 0
			s.highestTermSeen = rafttypes.Term(term)
		case s.highestTermSeen == 0:
			return 0, 0, errors.New("bug in protocol")
		}
		totalSize := rafttypes.Index(size + n1)
		if s.nextLogIndexInFile == rafttypes.Index(len(s.log)) {
			if s.currentFile != nil {
				if err := s.currentFile.Close(); err != nil {
					return 0, 0, err
				}
			}

			var err error
			s.currentFile, err = s.repo.Create(s.repo.LastTerm(), s.nextLogIndex, nil)
			if err != nil {
				return 0, 0, err
			}
			s.log, err = s.currentFile.Map()
			if err != nil {
				return 0, 0, err
			}
			s.nextLogIndexInFile = 0
		}
		n := rafttypes.Index(copy(s.log[s.nextLogIndexInFile:], data[:totalSize]))
		s.nextLogIndexInFile += n
		s.nextLogIndex += n
		if s.nextLogIndexInFile < rafttypes.Index(len(s.log)) {
			s.log[s.nextLogIndexInFile] = 0x00 // To clean potential garbage left due to power outage.
		}
		if n < totalSize {
			if s.currentFile != nil {
				if err := s.currentFile.Close(); err != nil {
					return 0, 0, err
				}
			}

			var err error
			s.currentFile, err = s.repo.Create(s.repo.LastTerm(), s.nextLogIndex, data[n:totalSize])
			if err != nil {
				return 0, 0, err
			}
			s.log, err = s.currentFile.Map()
			if err != nil {
				return 0, 0, err
			}
			s.nextLogIndexInFile = totalSize - n
			s.nextLogIndex += totalSize - n
		}
		data = data[totalSize:]
	}
	return s.repo.LastTerm(), s.nextLogIndex, nil
}

func (s *State) close() error {
	if s.currentFile != nil {
		return s.currentFile.Close()
	}
	return nil
}

func appendDefer(err *error) {
	if recover() != nil {
		*err = errors.New("invalid transaction")
	}
}

func findNextIndex(log []byte, txOffset rafttypes.Index) rafttypes.Index {
	index := txOffset
	for {
		if !varuint64.Contains(log[index:]) {
			return index
		}
		size, n := varuint64.Parse(log[index:])
		if size == 0 {
			return index
		}
		size += n
		if index+rafttypes.Index(size) > rafttypes.Index(len(log)) {
			return index
		}
		if !format.VerifyChecksum(log[index+rafttypes.Index(n) : index+rafttypes.Index(size)]) {
			return index
		}
		index += rafttypes.Index(size)
		if index == rafttypes.Index(len(log)) {
			return index
		}
	}
}
