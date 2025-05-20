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

	var nextIndex, nextIndexInFile types.Index
	var previousChecksum uint64
	var highestTermSeen rafttypes.Term

	var log []byte
	if currentFile != nil {
		log, err = currentFile.Map()
		if err != nil {
			_ = currentFile.Close()
			return nil, nil, err
		}

		header := currentFile.Header()
		prevChecksum, err := readChecksum(repo, header.NextIndex)
		if err != nil {
			_ = currentFile.Close()
			return nil, nil, err
		}

		highestTermSeen = header.Term
		nextIndexInFile, previousChecksum = initialize(log, prevChecksum)
		nextIndex = header.NextIndex + nextIndexInFile
	}

	s := &State{
		repo:             repo,
		em:               em,
		evState:          em.State(),
		nextIndex:        nextIndex,
		previousChecksum: previousChecksum,
		currentFile:      currentFile,
		log:              log,
		nextIndexInFile:  nextIndexInFile,
		highestTermSeen:  highestTermSeen,
	}
	return s, s.close, nil
}

// State represents the persistent state of the Raft consensus algorithm.
type State struct {
	repo             *repository.Repository
	em               *events.Store
	evState          events.State
	nextIndex        types.Index
	previousChecksum uint64

	currentFile     *repository.File
	log             []byte
	nextIndexInFile types.Index

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

// LastTerm returns the term of the last log entry in the state.
// If the log is empty, it returns 0.
func (s *State) LastTerm() rafttypes.Term {
	return s.repo.LastTerm()
}

// NextIndex returns the index of the next log entry.
// This is calculated based on the current length of the log.
// It effectively points to the position where a new log entry would be appended.
func (s *State) NextIndex() types.Index {
	return s.nextIndex
}

// PreviousTerm returns term of previous log item.
func (s *State) PreviousTerm(index types.Index) (rafttypes.Term, types.Index) {
	return s.repo.PreviousTerm(index)
}

// AppendTerm appends term to the log.
func (s *State) AppendTerm() (rafttypes.Term, types.Index, error) {
	var termEntry [2 * varuint64.MaxSize]byte

	n := varuint64.Put(termEntry[varuint64.MaxSize:], uint64(s.evState.Term))
	n2 := varuint64.Size(n)
	varuint64.Put(termEntry[varuint64.MaxSize-n2:], n)

	return s.appendTx(termEntry[varuint64.MaxSize-n2:varuint64.MaxSize+n], false, true)
}

// Validate validates the common point in log.
func (s *State) Validate(
	lastTerm rafttypes.Term,
	nextIndex types.Index,
) (rafttypes.Term, types.Index, bool, error) {
	if nextIndex == 0 {
		return s.validate(lastTerm, nextIndex)
	}
	if lastTerm == 0 {
		return 0, 0, false, errors.New("bug in protocol")
	}

	if nextIndex <= s.nextIndex {
		previousTerm, _ := s.PreviousTerm(nextIndex)
		if previousTerm == lastTerm {
			return s.validate(lastTerm, nextIndex)
		}
	}

	previousTerm, _ := s.PreviousTerm(s.nextIndex)
	return previousTerm, s.nextIndex, false, nil
}

// Append appends data to log.
func (s *State) Append(
	tx []byte,
	containsChecksum bool,
	allowTermMark bool,
) (rafttypes.Term, types.Index, error) {
	return s.appendTx(tx, containsChecksum, allowTermMark)
}

// Sync syncs data to persistent storage.
func (s *State) Sync() (types.Index, error) {
	if s.currentFile != nil {
		if err := s.currentFile.Sync(); err != nil {
			return 0, err
		}
	}
	return s.nextIndex, nil
}

func (s *State) validate(
	lastTerm rafttypes.Term,
	nextIndex types.Index,
) (rafttypes.Term, types.Index, bool, error) {
	if s.evState.Term == 0 {
		return 0, 0, false, errors.New("bug in protocol")
	}
	if nextIndex > s.nextIndex {
		// FIXME (wojciech): This is not tested.
		return 0, 0, false, errors.New("bug in protocol")
	}
	if nextIndex == 0 && lastTerm != 0 {
		// FIXME (wojciech): This is not tested.
		return 0, 0, false, errors.New("bug in protocol")
	}
	if nextIndex != 0 && lastTerm == 0 {
		// FIXME (wojciech): This is not tested.
		return 0, 0, false, errors.New("bug in protocol")
	}
	previousTerm, _ := s.PreviousTerm(nextIndex)
	if previousTerm != lastTerm {
		// FIXME (wojciech): This is not tested.
		return 0, 0, false, errors.New("bug in protocol")
	}

	lastTerm = s.repo.Revert(nextIndex)
	if nextIndex < s.nextIndex {
		if s.currentFile != nil {
			if err := s.currentFile.Close(); err != nil {
				return 0, 0, false, err
			}
			s.currentFile = nil
		}

		s.nextIndex = nextIndex

		var err error
		s.previousChecksum, err = readChecksum(s.repo, s.nextIndex)
		if err != nil {
			return 0, 0, false, err
		}
	}

	return lastTerm, s.nextIndex, true, nil
}

func (s *State) appendTx(
	data []byte,
	containsChecksum bool,
	allowTermMark bool,
) (_ rafttypes.Term, _ types.Index, retErr error) {
	if s.evState.Term == 0 {
		// FIXME (wojciech): This is not tested.
		return 0, 0, errors.New("bug in protocol")
	}

	defer appendDefer(&retErr)

	for len(data) > 0 {
		size, n1 := varuint64.Parse(data)
		if size == 0 {
			// FIXME (wojciech): This is not tested.
			return 0, 0, errors.New("bug in protocol")
		}
		if size > uint64(len(data[n1:])) {
			// FIXME (wojciech): This is not tested.
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

		//nolint:nestif
		if varuint64.Contains(data[n1:]) {
			term, n2 := varuint64.Parse(data[n1:])
			if n2 == sizeWithoutChecksum {
				if !allowTermMark {
					return 0, 0, errors.New("term mark not allowed")
				}

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
				s.currentFile, err = s.repo.Create(rafttypes.Term(term), s.nextIndex)
				if err != nil {
					return 0, 0, err
				}
				s.log, err = s.currentFile.Map()
				if err != nil {
					return 0, 0, err
				}
				s.nextIndexInFile = 0
				s.highestTermSeen = rafttypes.Term(term)
			}
		}
		if s.highestTermSeen == 0 {
			// FIXME (wojciech): This is not tested.
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

		if err := s.append(buf); err != nil {
			return 0, 0, err
		}

		data = data[n1+size:]
		s.previousChecksum = checksum
	}
	return s.repo.LastTerm(), s.nextIndex, nil
}

func (s *State) append(data []byte) error {
	if s.currentFile == nil {
		var err error
		s.currentFile, err = s.repo.Create(s.repo.LastTerm(), s.nextIndex)
		if err != nil {
			return err
		}
		s.log, err = s.currentFile.Map()
		if err != nil {
			return err
		}
		s.nextIndexInFile = 0
	}

	//nolint:nestif
	if len(data) > len(s.log[s.nextIndexInFile:]) {
		if s.currentFile != nil {
			if err := s.currentFile.Close(); err != nil {
				return err
			}
		}

		var err error
		s.currentFile, err = s.repo.Create(s.repo.LastTerm(), s.nextIndex)
		if err != nil {
			return err
		}
		s.log, err = s.currentFile.Map()
		if err != nil {
			return err
		}
		s.nextIndexInFile = 0
	}

	n := types.Index(copy(s.log[s.nextIndexInFile:], data))
	s.nextIndexInFile += n
	s.nextIndex += n

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

func initialize(log []byte, previousChecksum uint64) (types.Index, uint64) {
	var index types.Index
	for {
		if !varuint64.Contains(log[index:]) {
			return index, previousChecksum
		}
		size, n := varuint64.Parse(log[index:])
		if size == 0 {
			return index, previousChecksum
		}
		size += n
		if index+types.Index(size) > types.Index(len(log)) {
			return index, previousChecksum
		}

		txRaw := log[index : index+types.Index(size)]
		i := len(txRaw) - format.ChecksumSize
		checksum := xxh3.HashSeed(txRaw[:i], previousChecksum)
		if binary.LittleEndian.Uint64(txRaw[i:]) != checksum {
			return index, previousChecksum
		}
		index += types.Index(size)
		previousChecksum = checksum
		if index == types.Index(len(log)) {
			return index, previousChecksum
		}
	}
}

func readChecksum(repo *repository.Repository, nextIndex types.Index) (uint64, error) {
	if nextIndex == 0 {
		return 0, nil
	}

	file, err := repo.OpenByIndex(nextIndex - format.ChecksumSize)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var rawChecksum [format.ChecksumSize]byte
	if _, err := file.Reader().Read(rawChecksum[:]); err != nil {
		return 0, errors.WithStack(err)
	}

	return binary.LittleEndian.Uint64(rawChecksum[:]), nil
}
