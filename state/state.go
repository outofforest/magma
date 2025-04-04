package state

import (
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/varuint64"
)

// Closer represents function closing the state.
type Closer func() error

// New creates new state manager.
func New(repo *repository.Repository, em *events.Store) (*State, Closer, error) {
	currentFile, err := repo.OpenCurrent()
	if err != nil {
		return nil, nil, err
	}

	var nextLogIndex, nextLogIndexInFile rafttypes.Index
	var previousChecksum uint64
	var highestTermSeen rafttypes.Term

	var log []byte
	if currentFile != nil {
		var err error
		log, err = currentFile.Map()
		if err != nil {
			_ = currentFile.Close()
			return nil, nil, err
		}

		header := currentFile.Header()
		highestTermSeen = header.Term
		nextLogIndexInFile, previousChecksum = initialize(log, header.NextTxOffset, header.PreviousChecksum)
		nextLogIndex = header.NextLogIndex + nextLogIndexInFile
	}

	s := &State{
		repo:               repo,
		em:                 em,
		evState:            em.State(),
		nextLogIndex:       nextLogIndex,
		previousChecksum:   previousChecksum,
		currentFile:        currentFile,
		log:                log,
		nextLogIndexInFile: nextLogIndexInFile,
		highestTermSeen:    highestTermSeen,
	}
	return s, s.close, nil
}

// State represents the persistent state of the Raft consensus algorithm.
type State struct {
	repo             *repository.Repository
	em               *events.Store
	evState          events.State
	nextLogIndex     rafttypes.Index
	previousChecksum uint64

	currentFile        *repository.File
	log                []byte
	nextLogIndexInFile rafttypes.Index

	highestTermSeen rafttypes.Term
}

// CurrentTerm returns the current term of the state.
// The term represents a monotonically increasing number identifying
// the current term in the raft consensus algorithm.
func (s *State) CurrentTerm() rafttypes.Term {
	return s.evState.Term
}

// SetCurrentTerm sets the current term for the state.
// The term provided must be greater than the current term; otherwise, it returns an error,
// since a lower or equal term indicates a protocol inconsistency.
// Setting a new term also resets the votedFor field to ZeroServerID.
func (s *State) SetCurrentTerm(term rafttypes.Term) error {
	var err error
	s.evState, err = s.em.Term(term)
	return err
}

// VoteFor records a vote for a given candidate in the current term.
// It ensures that a vote is only recorded if no vote has been cast yet, or if
// the vote is consistent with the previously cast vote. A vote for the ZeroServerID
// is not allowed, as it would indicate a protocol inconsistency.
// Returns true if the vote was successfully recorded, or false if the vote
// was not recorded due to a prior vote for a different candidate.
func (s *State) VoteFor(candidate types.ServerID) (bool, error) {
	if s.evState.VotedFor != types.ZeroServerID && s.evState.VotedFor != candidate {
		return false, nil
	}

	if s.evState.VotedFor == types.ZeroServerID {
		var err error
		s.evState, err = s.em.Vote(candidate)
		if err != nil {
			return false, err
		}
	}

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
	var termEntry [2 * varuint64.MaxSize]byte

	n := varuint64.Put(termEntry[varuint64.MaxSize:], uint64(s.evState.Term))
	n2 := varuint64.Size(n)
	varuint64.Put(termEntry[varuint64.MaxSize-n2:], n)

	entry := termEntry[varuint64.MaxSize-n2 : varuint64.MaxSize+n]
	return s.appendTx(entry, false, true)
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

	lastLogTerm, nextLogIndex, err := s.repo.Revert(s.PreviousTerm(nextLogIndex) - 1)
	if err != nil {
		return 0, 0, err
	}
	s.nextLogIndex = nextLogIndex
	return lastLogTerm, nextLogIndex, nil
}

// Append appends data to log.
func (s *State) Append(
	tx []byte,
	containsChecksum bool,
	allowTermMark bool,
) (rafttypes.Term, rafttypes.Index, error) {
	return s.appendTx(tx, containsChecksum, allowTermMark)
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
	if s.evState.Term == 0 {
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

func (s *State) appendTx(
	data []byte,
	containsChecksum bool,
	allowTermMark bool,
) (_ rafttypes.Term, _ rafttypes.Index, retErr error) {
	if s.evState.Term == 0 {
		return 0, 0, errors.New("bug in protocol")
	}

	defer appendDefer(&retErr)

	for len(data) > 0 {
		size, n1 := varuint64.Parse(data)
		if size == 0 {
			return 0, 0, errors.New("bug in protocol")
		}
		if size > uint64(len(data[n1:])) {
			return 0, 0, errors.New("bug in protocol")
		}

		var sizeWithChecksum, sizeWithoutChecksum uint64
		if containsChecksum {
			if size < format.ChecksumSize {
				return 0, 0, errors.New("bad size")
			}
			sizeWithChecksum = size
			sizeWithoutChecksum = size - format.ChecksumSize
		} else {
			sizeWithChecksum = size + format.ChecksumSize
			sizeWithoutChecksum = size
		}

		if sizeWithChecksum > s.repo.PageCapacity() {
			return 0, 0, errors.New("tx size exceeds page size")
		}

		term, n2 := varuint64.Parse(data[n1:])
		switch {
		case n2 == sizeWithoutChecksum:
			if !allowTermMark {
				return 0, 0, errors.New("term mark not allowed")
			}

			// This is a term mark.
			if rafttypes.Term(term) <= s.highestTermSeen {
				return 0, 0, errors.New("bug in protocol")
			}
			if rafttypes.Term(term) > s.evState.Term {
				return 0, 0, errors.New("bug in protocol")
			}

			if s.currentFile != nil {
				if err := s.currentFile.Close(); err != nil {
					return 0, 0, err
				}
			}

			var err error
			s.currentFile, err = s.repo.Create(rafttypes.Term(term), s.nextLogIndex, s.previousChecksum, nil)
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

		var sizeBuf [varuint64.MaxSize]byte
		n := varuint64.Put(sizeBuf[:], sizeWithChecksum)

		hasher := xxh3.NewSeed(s.previousChecksum)
		if _, err := hasher.Write(sizeBuf[:n]); err != nil {
			return 0, 0, errors.WithStack(err)
		}
		if _, err := hasher.Write(data[n1 : n1+sizeWithoutChecksum]); err != nil {
			return 0, 0, errors.WithStack(err)
		}
		checksum := hasher.Sum64()
		if containsChecksum && binary.LittleEndian.Uint64(data[n1+sizeWithoutChecksum:n1+sizeWithChecksum]) != checksum {
			return 0, 0, errors.New("checksum mismatch")
		}

		var hashBuf [8]byte
		binary.LittleEndian.PutUint64(hashBuf[:], checksum)

		buf := make([]byte, n+sizeWithChecksum)
		copy(buf, sizeBuf[:n])
		copy(buf[n:], data[n1:n1+sizeWithoutChecksum])
		copy(buf[len(buf)-8:], hashBuf[:])

		//nolint:nestif
		if s.nextLogIndexInFile == rafttypes.Index(len(s.log)) {
			if s.currentFile != nil {
				if err := s.currentFile.Close(); err != nil {
					return 0, 0, err
				}
			}

			var err error
			s.currentFile, err = s.repo.Create(s.repo.LastTerm(), s.nextLogIndex, s.previousChecksum, nil)
			if err != nil {
				return 0, 0, err
			}
			s.log, err = s.currentFile.Map()
			if err != nil {
				return 0, 0, err
			}
			s.nextLogIndexInFile = 0
		}

		if err := s.append(buf, checksum); err != nil {
			return 0, 0, err
		}

		data = data[n1+size:]
		s.previousChecksum = checksum
	}
	return s.repo.LastTerm(), s.nextLogIndex, nil
}

func (s *State) append(data []byte, previousChecksum uint64) error {
	n := rafttypes.Index(copy(s.log[s.nextLogIndexInFile:], data))
	s.nextLogIndexInFile += n
	s.nextLogIndex += n

	//nolint:nestif
	if totalLen := rafttypes.Index(len(data)); n < totalLen {
		if s.currentFile != nil {
			if err := s.currentFile.Close(); err != nil {
				return err
			}
		}

		var err error
		s.currentFile, err = s.repo.Create(s.repo.LastTerm(), s.nextLogIndex, previousChecksum, data[n:])
		if err != nil {
			return err
		}
		s.log, err = s.currentFile.Map()
		if err != nil {
			return err
		}
		s.nextLogIndexInFile = totalLen - n
		s.nextLogIndex += totalLen - n
	}

	return nil
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

func initialize(log []byte, txOffset rafttypes.Index, previousChecksum uint64) (rafttypes.Index, uint64) {
	index := txOffset
	for {
		if !varuint64.Contains(log[index:]) {
			return index, previousChecksum
		}
		size, n := varuint64.Parse(log[index:])
		if size == 0 {
			return index, previousChecksum
		}
		size += n
		if index+rafttypes.Index(size) > rafttypes.Index(len(log)) {
			return index, previousChecksum
		}

		txRaw := log[index : index+rafttypes.Index(size)]
		i := len(txRaw) - format.ChecksumSize
		checksum := xxh3.HashSeed(txRaw[:i], previousChecksum)
		if binary.LittleEndian.Uint64(txRaw[i:]) != checksum {
			return index, previousChecksum
		}
		index += rafttypes.Index(size)
		previousChecksum = checksum
		if index == rafttypes.Index(len(log)) {
			return index, previousChecksum
		}
	}
}
