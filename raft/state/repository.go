package state

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"github.com/outofforest/magma/raft/state/format"
	"github.com/outofforest/magma/raft/types"
)

const maxHeaderSize = 1024

type File struct {
	pageSize uint64
	file     *os.File
	data     []byte
}

func (f *File) Reader() io.Reader {
	return f.file
}

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

func (f *File) Sync() error {
	if f.data != nil {
		if err := unix.Msync(f.data, unix.MS_SYNC); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (f *File) Close() error {
	if f.data != nil {
		if err := unix.Munmap(f.data); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(f.file.Close())
}

func NewRepository(dir string, pageSize uint64) (*Repository, error) {
	if pageSize == 0 || pageSize%uint64(os.Getpagesize()) != 0 || pageSize <= maxHeaderSize {
		return nil, errors.New("invalid page size")
	}

	m := format.NewMarshaller()

	files, err := listFiles(dir, m)
	if err != nil {
		return nil, err
	}

	return &Repository{
		m:             m,
		dir:           dir,
		pageSize:      pageSize,
		files:         files,
		nextFileIndex: uint64(len(files)),
	}, nil
}

type Repository struct {
	m        format.Marshaller
	dir      string
	pageSize uint64

	mu            sync.RWMutex
	files         []fileInfo
	nextFileIndex uint64
}

func (r *Repository) Create(term types.Term, nextLogIndex types.Index, remainingData []byte) (_ *File, retErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term == 0 {
		return nil, errors.New("invalid term")
	}
	header := &format.Header{
		Term:         term,
		NextLogIndex: nextLogIndex,
		TxOffset:     types.Index(len(remainingData)),
	}

	var headerBuf [maxHeaderSize]byte
	_, n, err := r.m.Marshal(header, headerBuf[:])
	if err != nil {
		return nil, err
	}

	header.Checksum = xxhash.Sum64(headerBuf[:n])

	_, n, err = r.m.Marshal(header, headerBuf[:])
	if err != nil {
		return nil, err
	}

	if n+uint64(len(remainingData)) > r.pageSize {
		return nil, errors.New("data size exceeds the page size")
	}

	var fileIndex uint64
	if len(r.files) > 0 {
		if term < r.files[len(r.files)-1].Header.Term {
			return nil, errors.New("cannot create file with past term")
		}
		fileIndex = r.files[len(r.files)-1].Index + 1
		header.PreviousTerm = r.files[len(r.files)-1].Header.Term
	} else if err := os.MkdirAll(r.dir, 0o700); err != nil {
		return nil, errors.WithStack(err)
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
	r.nextFileIndex++

	if _, err := f.Write(headerBuf[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	if len(remainingData) > 0 {
		if _, err := f.Write(remainingData); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if err := f.Sync(); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := os.Rename(fileNameTmp, fileName); err != nil {
		return nil, errors.WithStack(err)
	}

	r.files = append(r.files, fileInfo{
		Index:  fileIndex,
		Header: header,
	})

	file := &File{
		pageSize: r.pageSize,
		file:     f,
	}

	return file, nil
}

func (r *Repository) OpenCurrent() (*File, types.Index, error) {
	if len(r.files) == 0 {
		return nil, 0, nil
	}

	file, _, err := r.open(uint64(len(r.files)-1), 0)
	return file, r.files[len(r.files)-1].Header.TxOffset, err
}

func (r *Repository) Iterator(offset types.Index) *FileIterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var fileIndex uint64
	for i := len(r.files) - 1; i >= 0; i-- {
		if offset >= r.files[i].Header.NextLogIndex {
			fileIndex = uint64(i)
			offset -= r.files[i].Header.NextLogIndex
			break
		}
	}

	return &FileIterator{
		r:         r,
		fileIndex: fileIndex,
		offset:    uint64(offset),
	}
}

func (r *Repository) Revert(term types.Term) (types.Term, types.Index, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.files) == 0 {
		return 0, 0, errors.New("nothing to revert")
	}

	if term >= r.files[len(r.files)-1].Header.Term {
		return 0, 0, errors.New("nothing to revert")
	}

	for i := len(r.files) - 1; i >= 0; i-- {
		header := r.files[i].Header
		if header.PreviousTerm <= term {
			r.files = r.files[:i]
			return header.PreviousTerm, header.NextLogIndex, nil
		}
	}

	return 0, 0, errors.New("nothing to revert")
}

func (r *Repository) LastTerm() types.Term {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.files) == 0 {
		return 0
	}

	return r.files[len(r.files)-1].Header.Term
}

func (r *Repository) PreviousTerm(index types.Index) types.Term {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for i := len(r.files) - 1; i >= 0; i-- {
		header := r.files[i].Header
		if header.NextLogIndex < index {
			return header.Term
		}
	}

	return 0
}

func (r *Repository) open(fileIndex, offset uint64) (*File, types.Index, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fileIndex >= uint64(len(r.files)) {
		return nil, 0, errors.Errorf("file index out of range: %d", fileIndex)
	}

	if offset > r.pageSize {
		return nil, 0, errors.Errorf("offset %d is greater than page size %d", offset, r.pageSize)
	}

	f, err := os.OpenFile(r.filePath(fileIndex), os.O_RDWR, 0o600)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	switch uint64(size) {
	case r.pageSize:
		if _, err := f.Seek(maxHeaderSize, io.SeekStart); err != nil {
			return nil, 0, errors.WithStack(err)
		}
	default:
		return nil, 0, errors.Errorf("unexpected file size %d", size)
	}

	file := &File{
		pageSize: r.pageSize,
		file:     f,
	}

	if fileIndex == uint64(len(r.files)-1) {
		return file, r.files[fileIndex].Header.NextLogIndex + types.Index(r.pageSize) - maxHeaderSize, nil
	}

	return file, r.files[fileIndex+1].Header.NextLogIndex, nil
}

func (r *Repository) filePath(fileIndex uint64) string {
	return filepath.Join(r.dir, strconv.FormatUint(fileIndex, 10))
}

type FileIterator struct {
	r         *Repository
	fileIndex uint64
	offset    uint64
}

func (i *FileIterator) Next() (*File, types.Index, error) {
	s, validUntil, err := i.r.open(i.fileIndex, i.offset)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	i.fileIndex++
	i.offset = 0

	return s, validUntil, nil
}

func (i *FileIterator) Close() error {
	return nil
}

type fileInfo struct {
	Index  uint64
	Header *format.Header
}

func listFiles(dir string, m format.Marshaller) ([]fileInfo, error) {
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
		if _, err := strconv.ParseUint(d.Name(), 10, 64); err != nil {
			return errors.Wrapf(err, "invalid file name: %s", path)
		}

		fileIndex++

		return nil
	})
	if err != nil {
		return nil, err
	}

	if fileIndex == 0 {
		return nil, nil //nolint:nilnil
	}

	res := []fileInfo{}

	var previousHeader *format.Header
	for fileIndex--; fileIndex >= 0; fileIndex-- {
		header, err := readFileHeader(filepath.Join(dir, strconv.Itoa(fileIndex)), m)
		if err != nil {
			return nil, err
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

	for i := 0; i < len(res)/2; i++ {
		res[i], res[len(res)-i-1] = res[len(res)-i-1], res[i]
	}

	return res, nil
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

	checksum := header.Checksum
	header.Checksum = 0

	_, size, err := m.Marshal(header, headerBuf[:])
	if err != nil {
		return nil, err
	}
	if xxhash.Sum64(headerBuf[:size]) != checksum {
		return nil, errors.Errorf("checksum mismatch")
	}

	return header, nil
}
