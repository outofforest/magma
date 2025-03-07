package state

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const pageSize = 1024 * 1024

func OpenBook(dir string) (*Book, func() error, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	bookF, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if err := bookF.Truncate(int64(100 * pageSize)); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	b := &Book{
		f:     bookF,
		pages: map[uint64]*Page{},
	}
	return b, b.close, nil
}

type Book struct {
	f     *os.File
	pages map[uint64]*Page
	dirty []*Page
}

func (b *Book) Open(index uint64) (*Page, func() error, error) {
	pageIndex := index / pageSize
	p, err := b.page(pageIndex)
	if err != nil {
		return nil, nil, err
	}
	p.lock()

	return p, p.unlock, nil
}

func (b *Book) Sync() error {
	for _, p := range b.dirty {
		// If page is released isDirty is set to false but it stays in b.dirty.
		if p.isDirty {
			if err := p.sync(); err != nil {
				return err
			}
		}
	}

	b.dirty = b.dirty[:0]

	return nil
}

func (b *Book) page(pageIndex uint64) (*Page, error) {
	p := b.pages[pageIndex]
	if p == nil {
		data, err := unix.Mmap(int(b.f.Fd()), int64(pageIndex*pageSize), pageSize, unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_SHARED)
		if err != nil {
			return nil, errors.Wrapf(err, "file mapping failed")
		}
		p = &Page{
			book:  b,
			index: pageIndex,
			data:  data,
		}
		b.pages[pageIndex] = p
	}
	return p, nil
}

func (b *Book) close() error {
	defer clear(b.pages)

	for _, p := range b.pages {
		if err := p.close(); err != nil {
			return err
		}
	}

	b.dirty = b.dirty[:0]

	return nil
}

type Page struct {
	book    *Book
	index   uint64
	data    []byte
	locks   uint64
	isDirty bool
}

func (p *Page) Get(offset, length uint64) ([]byte, error) {
	pageIndex := offset / pageSize
	if pageIndex != p.index {
		return nil, errors.New("offset out of page range")
	}
	offset -= pageIndex * pageSize
	if offset+length >= pageSize {
		return p.data[offset:], nil
	}
	return p.data[offset : offset+length], nil
}

func (p *Page) Set(offset uint64, data []byte) (uint64, error) {
	pageIndex := offset / pageSize
	if pageIndex != p.index {
		return 0, errors.New("offset out of page range")
	}
	p.isDirty = true
	return uint64(copy(p.data[offset-pageIndex*pageSize:], data)), nil
}

func (p *Page) lock() {
	p.locks++
}

func (p *Page) unlock() error {
	if p.locks == 0 {
		return errors.New("unlocking released page")
	}
	p.locks--
	if p.locks == 0 {
		delete(p.book.pages, p.index)
		return p.close()
	}

	return nil
}

func (p *Page) close() error {
	p.locks = 0
	p.isDirty = false
	if err := unix.Munmap(p.data); err != nil {
		return errors.Wrapf(err, "file mapping failed")
	}
	return nil
}

func (p *Page) sync() error {
	if p.locks == 0 {
		return errors.New("syncing released page")
	}

	if err := unix.Msync(p.data, unix.MS_SYNC); err != nil {
		return errors.Wrap(err, "page syncing failed")
	}
	p.isDirty = false

	return nil
}
