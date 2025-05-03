package events

import (
	"bufio"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"

	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/events/codec"
	"github.com/outofforest/magma/state/events/format"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/proton"
)

const termsPerFile = 2000

// State represents the actual state recreated from events.
type State struct {
	Term     rafttypes.Term
	VotedFor magmatypes.ServerID
}

func (s State) term(term rafttypes.Term) State {
	s.Term = term
	s.VotedFor = magmatypes.ZeroServerID
	return s
}

func (s State) vote(candidate magmatypes.ServerID) State {
	s.VotedFor = candidate
	return s
}

// Open opens event database.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, errors.WithStack(err)
	}

	fileIndex, err := findFile(dir)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(filepath.Join(dir, strconv.FormatUint(fileIndex, 10)), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0o600)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	m := format.NewMarshaller()
	n, checksumSeed, s, err := state(codec.NewDecoder(bufio.NewReader(f), m))
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(int64(n), io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	return &Store{
		dir:       dir,
		f:         f,
		m:         m,
		fileIndex: fileIndex,
		encoder:   codec.NewEncoder(checksumSeed, f, m),
		s:         s,
	}, nil
}

// Store stores events.
type Store struct {
	dir       string
	f         *os.File
	m         proton.Marshaller
	fileIndex uint64
	encoder   *codec.Encoder
	s         State
}

// State gets the current state represented by the stored events.
func (s *Store) State() State {
	return s.s
}

// Term stores new term.
func (s *Store) Term(term rafttypes.Term) (State, error) {
	if term == 0 {
		return State{}, errors.New("invalid term")
	}
	if term <= s.s.Term {
		return State{}, errors.New("new term must be higher than the previous one")
	}
	fileIndex := uint64(term / termsPerFile)
	if fileIndex != s.fileIndex {
		if err := s.f.Close(); err != nil {
			return State{}, errors.WithStack(err)
		}
		var err error
		s.f, err = os.OpenFile(filepath.Join(s.dir, strconv.FormatUint(fileIndex, 10)),
			os.O_RDWR|os.O_CREATE|os.O_SYNC, 0o600)
		if err != nil {
			return State{}, errors.WithStack(err)
		}
		s.encoder = codec.NewEncoder(0, s.f, s.m)
		s.fileIndex = fileIndex
	}

	if err := s.encoder.Encode(&format.Term{Term: term}); err != nil {
		return State{}, err
	}
	s.s = s.s.term(term)
	return s.s, nil
}

// Vote stores new vote.
func (s *Store) Vote(candidate magmatypes.ServerID) (State, error) {
	if s.s.Term == 0 {
		return State{}, errors.New("vote must not be applied until term is set")
	}
	if s.s.VotedFor != magmatypes.ZeroServerID {
		return State{}, errors.New("double voting is forbidden")
	}
	if candidate == magmatypes.ZeroServerID {
		return State{}, errors.New("invalid candidate")
	}

	if err := s.encoder.Encode(&format.Vote{Candidate: candidate}); err != nil {
		return State{}, err
	}
	s.s = s.s.vote(candidate)
	return s.s, nil
}

// Close closes the store.
func (s *Store) Close() error {
	return errors.WithStack(s.f.Close())
}

func findFile(dir string) (uint64, error) {
	var fileIndex uint64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return filepath.SkipAll
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if d.IsDir() {
			if path == dir {
				return nil
			}
			return errors.Errorf("unexpected directory: %s", path)
		}
		fi, err := strconv.ParseUint(d.Name(), 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid file name: %s", path)
		}

		if fi > fileIndex {
			fileIndex = fi
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return fileIndex, nil
}

func state(decoder *codec.Decoder) (uint64, uint64, State, error) {
	var s State

	var processed, checksumSeed uint64
	for {
		n, chS, event, err := decoder.Decode()
		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			return processed, checksumSeed, s, nil
		default:
			return 0, 0, State{}, errors.WithStack(err)
		}

		processed = n
		checksumSeed = chS
		switch e := event.(type) {
		case *format.Term:
			s = s.term(e.Term)
		case *format.Vote:
			s = s.vote(e.Candidate)
		}
	}
}
