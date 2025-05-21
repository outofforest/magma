package repository

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/repository/format"
	magmatypes "github.com/outofforest/magma/types"
)

const maxHeaderSize = 1024

// File represents opened file.
type File struct {
	pageSize uint64
	header   *format.Header
	file     *os.File
	data     []byte
}

// Header returns the header of the file.
func (f *File) Header() format.Header {
	return *f.header
}

// Reader returns file reader.
func (f *File) Reader() io.Reader {
	return f.file
}

// Map maps the file content to the memory region.
func (f *File) Map() ([]byte, error) {
	if f.data == nil {
		var err error
		f.data, err = unix.Mmap(int(f.file.Fd()), 0, int(f.pageSize),
			unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			return nil, errors.Wrapf(err, "mapping failed")
		}
	}
	return f.data[maxHeaderSize:], nil
}

// Sync syncs changes made to the map.
func (f *File) Sync() error {
	if f.data != nil {
		if err := unix.Msync(f.data, unix.MS_SYNC); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// Close closes the file.
func (f *File) Close() error {
	if f.data != nil {
		if err := unix.Munmap(f.data); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(f.file.Close())
}

// Open opens the repository.
func Open(dir string, pageSize uint64) (*Repository, error) {
	if pageSize == 0 || pageSize%uint64(os.Getpagesize()) != 0 || pageSize <= maxHeaderSize {
		return nil, errors.New("invalid page size")
	}

	// This is done to allow implement versioning later without moving the files.
	dir = filepath.Join(dir, "0")

	m := format.NewMarshaller()

	files, nextFileIndex, err := listFiles(dir, m)
	if err != nil {
		return nil, err
	}

	return &Repository{
		m:             m,
		dir:           dir,
		pageSize:      pageSize,
		files:         files,
		nextFileIndex: nextFileIndex,
	}, nil
}

// Repository is a set of files storing transactions.
type Repository struct {
	m        format.Marshaller
	dir      string
	pageSize uint64

	mu            sync.RWMutex
	files         []fileInfo
	nextFileIndex uint64
}

// PageCapacity returns the capacity of single page in the repository.
func (r *Repository) PageCapacity() uint64 {
	return r.pageSize - maxHeaderSize
}

// Create creates new file.
func (r *Repository) Create(term types.Term, nextIndex magmatypes.Index) (_ *File, retErr error) {
	if term == 0 {
		return nil, errors.New("invalid term")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	header := &format.Header{
		Term:      term,
		NextIndex: nextIndex,
	}

	if len(r.files) > 0 {
		h := r.files[len(r.files)-1].Header
		if term < h.Term {
			return nil, errors.New("cannot create file with past term")
		}
		if nextIndex <= h.NextIndex {
			return nil, errors.New("cannot create file with same or lower index")
		}
		header.PreviousTerm = h.Term
	} else if err := os.MkdirAll(r.dir, 0o700); err != nil {
		return nil, errors.WithStack(err)
	}

	var headerBuf [maxHeaderSize]byte
	_, n, err := r.m.Marshal(header, headerBuf[:])
	if err != nil {
		return nil, err
	}

	header.HeaderChecksum = xxhash.Sum64(headerBuf[:n])

	_, _, err = r.m.Marshal(header, headerBuf[:])
	if err != nil {
		return nil, err
	}

	fileName := r.filePath(r.nextFileIndex)
	fileNameTmp := fileName + ".tmp"
	f, err := os.OpenFile(fileNameTmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	if err := f.Truncate(int64(r.pageSize)); err != nil {
		return nil, errors.WithStack(err)
	}

	if _, err := f.Write(headerBuf[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := f.Sync(); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := os.Rename(fileNameTmp, fileName); err != nil {
		return nil, errors.WithStack(err)
	}

	r.files = append(r.files, fileInfo{
		Index:  r.nextFileIndex,
		Header: header,
	})

	file := &File{
		header:   header,
		pageSize: r.pageSize,
		file:     f,
	}

	r.nextFileIndex++

	return file, nil
}

// OpenCurrent opens the latest file in the repository.
func (r *Repository) OpenCurrent() (*File, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.files) == 0 {
		return nil, nil //nolint:nilnil
	}

	return r.open(uint64(len(r.files)-1), 0)
}

// OpenByIndex returns a file corresponding to the provided index and seeks to this position.
func (r *Repository) OpenByIndex(index magmatypes.Index) (*File, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for i := len(r.files) - 1; i >= 0; i-- {
		if f := r.files[i]; f.Header.NextIndex <= index {
			offset := uint64(index - f.Header.NextIndex)
			if offset >= r.pageSize-maxHeaderSize {
				return nil, errors.New("index does not exist")
			}
			return r.open(uint64(i), offset)
		}
	}

	return nil, errors.New("index does not exist")
}

// Iterator returns file iterator.
func (r *Repository) Iterator(provider *TailProvider, offset magmatypes.Index) *Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var fileIndex uint64
	fileOffset := offset
	for i := len(r.files) - 1; i >= 0; i-- {
		file := r.files[i]
		if offset >= file.Header.NextIndex {
			fileIndex = uint64(i)
			fileOffset -= file.Header.NextIndex
			break
		}
	}

	return &Iterator{
		r:         r,
		provider:  provider,
		fileIndex: fileIndex,
		offset:    uint64(fileOffset),
		current:   offset,
	}
}

// Revert reverts repository to previous term.
func (r *Repository) Revert(nextIndex magmatypes.Index) types.Term {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := len(r.files) - 1; i >= 0; i-- {
		h := r.files[i].Header
		if h.NextIndex < nextIndex {
			r.files = r.files[:i+1]
			return h.Term
		}
	}

	r.files = r.files[:0]
	return 0
}

// LastTerm returns last stored term.
func (r *Repository) LastTerm() types.Term {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.files) == 0 {
		return 0
	}

	return r.files[len(r.files)-1].Header.Term
}

// PreviousTerm returns term of the previous index.
func (r *Repository) PreviousTerm(index magmatypes.Index) (types.Term, magmatypes.Index) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var term types.Term
	var startTerm types.Term
	var termStartIndex magmatypes.Index
	for i := len(r.files) - 1; i >= 0; i-- {
		h := r.files[i].Header
		if h.NextIndex < index && h.Term >= startTerm {
			startTerm = h.Term
			termStartIndex = h.NextIndex
		}
		if h.NextIndex < index && term == 0 {
			term = h.Term
		}
		if h.PreviousTerm < startTerm && term > 0 {
			break
		}
	}

	return term, termStartIndex
}

func (r *Repository) open(fileIndex, offset uint64) (*File, error) {
	if fileIndex >= uint64(len(r.files)) {
		return nil, errors.Errorf("file index out of range: %d", fileIndex)
	}

	if offset > r.pageSize {
		return nil, errors.Errorf("offset %d is greater than page size %d", offset, r.pageSize)
	}

	f, err := os.OpenFile(r.filePath(r.files[fileIndex].Index), os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch uint64(size) {
	case r.pageSize:
		if _, err := f.Seek(maxHeaderSize+int64(offset), io.SeekStart); err != nil {
			return nil, errors.WithStack(err)
		}
	default:
		return nil, errors.Errorf("unexpected file size %d", size)
	}

	header := r.files[fileIndex].Header

	return &File{
		header:   header,
		pageSize: r.pageSize,
		file:     f,
	}, nil
}

func (r *Repository) filePath(fileIndex uint64) string {
	return filepath.Join(r.dir, strconv.FormatUint(fileIndex, 10))
}

type fileInfo struct {
	Index  uint64
	Header *format.Header
}

func listFiles(dir string, m format.Marshaller) ([]fileInfo, uint64, error) {
	var fileIndex int
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
		if strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		if _, err := strconv.ParseUint(d.Name(), 10, 64); err != nil {
			return errors.Wrapf(err, "invalid file name: %s", path)
		}

		fileIndex++

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	if fileIndex == 0 {
		return nil, 0, nil
	}

	nextFileIndex := fileIndex

	res := []fileInfo{}

	var previousHeader *format.Header
	for fileIndex--; fileIndex >= 0; fileIndex-- {
		header, err := readFileHeader(filepath.Join(dir, strconv.Itoa(fileIndex)), m)
		if err != nil {
			return nil, 0, err
		}

		if len(res) == 0 {
			previousHeader = header
			res = append(res, fileInfo{
				Index:  uint64(fileIndex),
				Header: header,
			})
			continue
		}

		if header.Term != previousHeader.PreviousTerm {
			continue
		}

		previousHeader = header
		res = append(res, fileInfo{
			Index:  uint64(fileIndex),
			Header: header,
		})
	}

	for i := range len(res) / 2 {
		res[i], res[len(res)-i-1] = res[len(res)-i-1], res[i]
	}

	return res, uint64(nextFileIndex), nil
}

func readFileHeader(path string, m format.Marshaller) (*format.Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()

	var headerBuf [maxHeaderSize]byte
	n, err := f.Read(headerBuf[:])
	if err != nil {
		return nil, errors.WithStack(err)
	}

	id, err := m.ID((*format.Header)(nil))
	if err != nil {
		return nil, err
	}
	msg, _, err := m.Unmarshal(id, headerBuf[:n])
	if err != nil {
		return nil, err
	}

	header, ok := msg.(*format.Header)
	if !ok {
		return nil, errors.Errorf("unexpected message type: %T", msg)
	}

	checksum := header.HeaderChecksum
	header.HeaderChecksum = 0

	_, size, err := m.Marshal(header, headerBuf[:])
	if err != nil {
		return nil, err
	}
	if xxhash.Sum64(headerBuf[:size]) != checksum {
		return nil, errors.Errorf("checksum mismatch")
	}

	header.HeaderChecksum = checksum
	return header, nil
}
