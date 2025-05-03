package codec

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

const checksumSize = 8

// NewEncoder creates new encoder.
func NewEncoder(checksumSeed uint64, w io.Writer, m proton.Marshaller) *Encoder {
	return &Encoder{
		w:            w,
		m:            m,
		checksumSeed: checksumSeed,
	}
}

// Encoder encodes events to the output stream.
type Encoder struct {
	w io.Writer
	m proton.Marshaller

	buf          []byte
	checksumSeed uint64
}

// Encode encodes event.
func (e *Encoder) Encode(v any) error {
	id, err := e.m.ID(v)
	if err != nil {
		return err
	}
	size, err := e.m.Size(v)
	if err != nil {
		return err
	}

	vSize := size + varuint64.Size(id) + checksumSize
	totalSize := vSize + varuint64.Size(vSize)

	if uint64(len(e.buf)) < totalSize {
		e.buf = make([]byte, totalSize)
	}

	n1 := varuint64.Put(e.buf, vSize)
	n2 := varuint64.Put(e.buf[n1:], id)
	if _, _, err := e.m.Marshal(v, e.buf[n1+n2:]); err != nil {
		return err
	}

	checksum := xxh3.HashSeed(e.buf[:n1+n2+size], e.checksumSeed)
	binary.LittleEndian.PutUint64(e.buf[n1+n2+size:], checksum)
	e.checksumSeed = checksum

	_, err = e.w.Write(e.buf[:totalSize])
	return errors.WithStack(err)
}
