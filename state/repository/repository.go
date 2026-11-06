package repository

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/config"
	"github.com/outofforest/magma/db"
	"github.com/outofforest/magma/internal"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/repository/format"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
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

// Repository is a set of files storing transactions.
type Repository struct {
	m               format.Marshaller
	metaM           proton.Marshaller
	dir             string
	pageSize        uint64
	partitionConfig config.PartitionConfig
	maxMessageSize  uint64
	currentVersion  magmatypes.Version
	upgradeCh       chan struct{}

	mu            sync.RWMutex
	files         []*format.Header
	nextFileIndex uint64
}

// Open opens the repository.
func Open(
	dir string,
	pageSize uint64,
	partitionConfig config.PartitionConfig,
	maxMessageSize uint64,
) (*Repository, error) {
	if pageSize == 0 || pageSize%uint64(os.Getpagesize()) != 0 || pageSize <= maxHeaderSize {
		return nil, errors.New("invalid page size")
	}

	m := format.NewMarshaller()

	files, nextFileIndex, err := listFiles(dir, m)
	if err != nil {
		return nil, err
	}

	upgradeCh := make(chan struct{}, 1)
	upgradeCh <- struct{}{}

	r := &Repository{
		m:               m,
		metaM:           wire.NewMarshaller(),
		dir:             dir,
		pageSize:        pageSize,
		partitionConfig: partitionConfig,
		maxMessageSize:  maxMessageSize,
		upgradeCh:       upgradeCh,
		files:           files,
		nextFileIndex:   nextFileIndex,
	}

	if len(files) > 0 {
		r.currentVersion = files[len(files)-1].Version
	}

	return r, nil
}

// PageCapacity returns the capacity of single page in the repository.
func (r *Repository) PageCapacity() uint64 {
	return r.pageSize - maxHeaderSize
}

// Create creates new file.
func (r *Repository) Create(
	term types.Term,
	nextIndex magmatypes.Index,
	previousChecksum uint64,
) (_ *File, retErr error) {
	if term == 0 {
		return nil, errors.New("invalid term")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	header := &format.Header{
		Version:          r.currentVersion,
		FileIndex:        r.nextFileIndex,
		Term:             term,
		NextIndex:        nextIndex,
		PreviousChecksum: previousChecksum,
	}

	if len(r.files) > 0 {
		h := r.files[len(r.files)-1]
		if term < h.Term {
			return nil, errors.New("cannot create file with past term")
		}
		if nextIndex <= h.NextIndex {
			return nil, errors.New("cannot create file with same or lower index")
		}
		header.PreviousTerm = h.Term
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

	if err := os.MkdirAll(filepath.Dir(fileName), 0o700); err != nil {
		return nil, errors.WithStack(err)
	}

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

	file := &File{
		header:   header,
		pageSize: r.pageSize,
		file:     f,
	}

	r.nextFileIndex++
	r.files = append(r.files, header)

	if len(r.files) > 1 && r.files[len(r.files)-2].Version != r.currentVersion {
		if len(r.upgradeCh) > 0 {
			select {
			case <-r.upgradeCh:
			default:
			}
		}

		r.upgradeCh <- struct{}{}
	}

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

	for i, h := range slices.Backward(r.files) {
		if h.NextIndex <= index {
			offset := uint64(index - h.NextIndex)
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
	for i, h := range slices.Backward(r.files) {
		if offset >= h.NextIndex {
			fileIndex = uint64(i)
			fileOffset -= h.NextIndex
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

	for i, h := range slices.Backward(r.files) {
		if h.NextIndex < nextIndex {
			r.files = r.files[:i+1]
			return h.Term
		}
	}

	r.files = r.files[:0]
	return 0
}

// InitReader opens version init file for reading.
func (r *Repository) InitReader(version magmatypes.Version) (
	retI magmatypes.Index,
	retR io.ReadCloser,
	retS uint64,
	retErr error,
) {
	if version == 0 {
		return 0, nil, 0, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var header *format.Header
	for _, h := range slices.Backward(r.files) {
		if h.Version < version {
			break
		}
		header = h
	}
	if header == nil || header.Version != version {
		return 0, nil, 0, errors.Errorf("there is no version %d", version)
	}

	f, err := os.Open(filepath.Join(r.dir, fmt.Sprintf("%d.init", header.FileIndex)))
	switch {
	case err == nil:
	case os.IsNotExist(err):
		return 0, nil, 0, errors.Errorf("init does not exist for version %d", header.Version)
	default:
		return 0, nil, 0, errors.Wrapf(err, "failed to open init file for version %d", header.Version)
	}
	defer func() {
		if retErr != nil && f != nil {
			_ = f.Close()
		}
	}()

	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil, 0, errors.WithStack(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, nil, 0, errors.WithStack(err)
	}

	return header.NextIndex, f, uint64(size), nil
}

// SetCurrentVersion sets current version.
func (r *Repository) SetCurrentVersion(version magmatypes.Version) error {
	if version < r.currentVersion {
		return errors.Errorf("invalid version %d", version)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentVersion = version

	return nil
}

// LastTerm returns last stored term.
func (r *Repository) LastTerm() types.Term {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.files) == 0 {
		return 0
	}

	return r.files[len(r.files)-1].Term
}

// PreviousTerm returns term of the previous index.
func (r *Repository) PreviousTerm(index magmatypes.Index) (types.Term, magmatypes.Index) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var term types.Term
	var startTerm types.Term
	var termStartIndex magmatypes.Index
	for _, h := range slices.Backward(r.files) {
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

// Run runs task gnerating init file when version is upgraded.
func (r *Repository) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-r.upgradeCh:
		}

		if err := r.upgrade(ctx); err != nil {
			return err
		}
	}
}

func (r *Repository) upgrade(ctx context.Context) error {
	r.mu.RLock()
	if len(r.files) < 2 {
		r.mu.RUnlock()
		return nil
	}
	files := slices.Clone(r.files)
	r.mu.RUnlock()

	for i, h := range files[1:] {
		if h.Version == files[i].Version {
			continue
		}
		if err := r.upgradeToVersion(ctx, h, files[i].Version); err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) upgradeToVersion(
	ctx context.Context,
	header *format.Header,
	previousVersion magmatypes.Version,
) error {
	initFile := filepath.Join(r.dir, fmt.Sprintf("%d.init", header.FileIndex))
	_, err := os.Stat(initFile)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return err
	}

	initFileTmp := initFile + ".tmp"
	f, err := os.OpenFile(initFileTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	initialMarshaller := r.partitionConfig.InitialMarshaller
	if previousVersion > 0 {
		initialMarshaller = r.partitionConfig.Upgrades[previousVersion-1].Marshaller
	}

	if err := r.fillInitFile(ctx, f, header, previousVersion, initialMarshaller,
		r.partitionConfig.Upgrades[previousVersion:header.Version]); err != nil {
		return err
	}

	return errors.WithStack(os.Rename(initFileTmp, initFile))
}

//nolint:gocyclo
func (r *Repository) fillInitFile(
	ctx context.Context,
	f io.Writer,
	header *format.Header,
	previousVersion magmatypes.Version,
	initialMarshaller proton.Marshaller,
	upgrades []db.Upgrade,
) error {
	initEntityMetaMsgID, err := r.metaM.ID(&wire.InitEntityMetadata{})
	if err != nil {
		return err
	}

	byID, byType, err := internal.NewCache(initialMarshaller)
	if err != nil {
		return err
	}

	initialEntities, err := internal.EntitiesFromMarshaller(initialMarshaller)
	if err != nil {
		return err
	}

	dbConfig, err := internal.NewDBConfig(initialEntities, nil)
	if err != nil {
		return err
	}

	memDB, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return err
	}

	versionOffset, initReader, initSize, err := r.InitReader(previousVersion)
	if err != nil {
		return err
	}

	updatedTypes := map[reflect.Type]struct{}{}
	tx := memDB.Txn(true)

	var checksum uint64
	if initReader != nil {
		defer initReader.Close()

		var err error
		checksum, err = internal.ApplyInit(resonance.NewReader(initReader, r.maxMessageSize), initialMarshaller,
			r.metaM, byID, initSize, tx, updatedTypes)
		if err != nil {
			return err
		}
	}

	p := NewTailProvider()
	p.SetTail(header.NextIndex, 0)
	it := r.Iterator(p, versionOffset)

loop:
	for {
		itr, _, _, err := it.Reader(false)
		switch {
		case err != nil:
			return err
		case itr == nil:
			break loop
		default:
		}

		reader := resonance.NewReader(itr, r.maxMessageSize)

	loopReader:
		for {
			txRaw, _, err := reader.ReceiveRawBytes()
			switch {
			case err == nil:
			case errors.Is(err, io.EOF):
				break loopReader
			default:
				return err
			}

			_, checksum, err = internal.ApplyTx(initialMarshaller, r.metaM, byID, checksum, tx, txRaw, updatedTypes)
			if err != nil && !errors.Is(err, db.ErrTxOutdatedTx) {
				return err
			}
		}
	}

	tx.Commit()

	buf := make([]byte, r.maxMessageSize+2*varuint64.MaxSize)
	_, size, err := r.metaM.Marshal(&wire.InitMetadata{
		Version:          header.Version,
		Term:             header.Term,
		PreviousChecksum: header.PreviousChecksum,
		NextIndex:        header.NextIndex,
		EntityMetadataID: initEntityMetaMsgID,
	}, buf[varuint64.MaxSize:])
	if err != nil {
		return err
	}

	n := varuint64.Size(size)
	start := varuint64.MaxSize - n
	varuint64.Put(buf[start:], size)

	metaBuf := buf[start : varuint64.MaxSize+size]
	if _, err := f.Write(metaBuf); err != nil {
		return errors.WithStack(err)
	}

	srcView := db.NewView(memDB.Txn(true), byType)

	for i, upgrade := range upgrades {
		entities, err := internal.EntitiesFromMarshaller(upgrade.Marshaller)
		if err != nil {
			return err
		}

		dstDBConfig, err := internal.NewDBConfig(entities, nil)
		if err != nil {
			return err
		}

		dstMemDB, err := memdb.NewMemDB(dstDBConfig)
		if err != nil {
			return err
		}

		_, byType, err = internal.NewCache(upgrade.Marshaller)
		if err != nil {
			return err
		}

		isLastUpgrade := i == len(upgrades)-1
		dstView := db.NewView(dstMemDB.Txn(true), byType)

		err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			ch := make(chan any, 5)
			spawn("convert", parallel.Continue, func(_ context.Context) error {
				defer close(ch)

				return upgrade.UpgradeFunc(srcView, ch)
			})
			spawn("store", parallel.Exit, func(_ context.Context) error {
				defer func() {
					for range ch {
					}
				}()

				for e := range ch {
					eValue := reflect.ValueOf(e)
					eType := eValue.Type()
					ePtrValue := reflect.New(eType)
					ePtrValue.Elem().Set(eValue)

					typeDef, exists := byType[eType]
					if !exists {
						return errors.Errorf("undefined type %T", e)
					}

					if !isLastUpgrade {
						dstView.Set(e)
						continue
					}

					_, metaSize, err := r.metaM.Marshal(&wire.InitEntityMetadata{
						ID:        memdb.ID(internal.UnsafeIDFromEntity(ePtrValue.UnsafePointer())),
						MessageID: typeDef.MsgID,
					}, buf[varuint64.MaxSize:])
					if err != nil {
						return err
					}

					_, size, err := upgrade.Marshaller.Marshal(ePtrValue.Interface(), buf[varuint64.MaxSize+metaSize:])
					if err != nil {
						return err
					}
					totalSize := metaSize + size
					n := varuint64.Size(totalSize)
					start := varuint64.MaxSize - n
					varuint64.Put(buf[start:], totalSize)

					msgBuf := buf[start : varuint64.MaxSize+totalSize]
					if _, err := f.Write(msgBuf); err != nil {
						return errors.WithStack(err)
					}
				}

				return errors.WithStack(ctx.Err())
			})
			return nil
		})
		if err != nil {
			return err
		}

		srcView = dstView
	}

	return nil
}

func (r *Repository) open(fileIndex, offset uint64) (*File, error) {
	if fileIndex >= uint64(len(r.files)) {
		return nil, errors.Errorf("file index out of range: %d", fileIndex)
	}

	if offset > r.pageSize {
		return nil, errors.Errorf("offset %d is greater than page size %d", offset, r.pageSize)
	}

	f, err := os.OpenFile(r.filePath(r.files[fileIndex].FileIndex), os.O_RDWR, 0o600)
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

	return &File{
		header:   r.files[fileIndex],
		pageSize: r.pageSize,
		file:     f,
	}, nil
}

func (r *Repository) filePath(fileIndex uint64) string {
	return filepath.Join(r.dir, strconv.FormatUint(fileIndex, 10))
}

func listFiles(dir string, m format.Marshaller) ([]*format.Header, uint64, error) {
	headers := []*format.Header{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return filepath.SkipAll
		}
		if err != nil {
			return errors.WithStack(err)
		}

		name := d.Name()
		if d.IsDir() || strings.HasSuffix(name, ".tmp") || strings.HasSuffix(name, ".init") {
			return nil
		}
		if _, err := strconv.ParseUint(name, 10, 64); err != nil {
			return errors.Wrapf(err, "invalid file name: %s", path)
		}

		header, err := readFileHeader(path, m)
		if err != nil {
			return err
		}

		headers = append(headers, header)

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	nextFileIndex := len(headers)
	if nextFileIndex == 0 {
		return nil, 0, nil
	}

	sort.Slice(headers, func(i, j int) bool {
		return headers[i].FileIndex > headers[j].FileIndex
	})

	res := []*format.Header{}
	var previousHeader *format.Header
	for _, header := range headers {
		if len(res) == 0 {
			previousHeader = header
			res = append(res, header)
			continue
		}

		if header.Term != previousHeader.PreviousTerm {
			continue
		}

		previousHeader = header
		res = append(res, header)
	}

	for i := range len(res) / 2 {
		res[i], res[len(res)-i-1] = res[len(res)-i-1], res[i]
	}

	var version magmatypes.Version
	for _, h := range res {
		if h.Version == version {
			continue
		}
		if h.Version < version {
			return nil, 0, errors.Errorf("version mismatch in file %d", h.FileIndex)
		}

		_, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%d.init", h.FileIndex)))
		if err != nil {
			return nil, 0, errors.Wrap(err, "init file does not exist")
		}

		version = h.Version
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
