package codec

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

// NewDecoder creates new decoder.
func NewDecoder(r io.Reader, m proton.Marshaller) *Decoder {
	return &Decoder{
		r:   r,
		m:   m,
		buf: make([]byte, varuint64.MaxSize),
	}
}

// Decoder decodes events from the input stream.
type Decoder struct {
	r io.Reader
	m proton.Marshaller

	buf          []byte
	checksumSeed uint64
	count        uint64
}

// Decode decodes single event.
func (d *Decoder) Decode() (uint64, any, error) {
	var sizeReceived uint64
	for !varuint64.Contains(d.buf[:sizeReceived]) {
		n, err := d.r.Read(d.buf[sizeReceived : sizeReceived+1])
		if err != nil {
			return 0, nil, errors.WithStack(err)
		}
		sizeReceived += uint64(n)
	}

	size, n := varuint64.Parse(d.buf[:sizeReceived])
	if uint64(len(d.buf)) < size+n {
		buf := make([]byte, size+n)
		copy(buf, d.buf[:n])
		d.buf = buf
	}

	if _, err := io.ReadFull(d.r, d.buf[n:n+size]); err != nil {
		return 0, nil, errors.WithStack(err)
	}

	checksum := xxh3.HashSeed(d.buf[:n+size-checksumSize], d.checksumSeed)
	expectedChecksum := binary.LittleEndian.Uint64(d.buf[n+size-checksumSize:])

	if checksum != expectedChecksum {
		return 0, nil, errors.WithStack(io.EOF)
	}

	d.checksumSeed = checksum

	id, n2 := varuint64.Parse(d.buf[n:])
	v, _, err := d.m.Unmarshal(id, d.buf[n+n2:n+size])
	if err != nil {
		return 0, nil, err
	}
	d.count += n + size

	return d.count, v, nil
}
