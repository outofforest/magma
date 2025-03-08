package hello

import (
	"unsafe"

	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id0 uint64 = iota + 1
)

var _ proton.Marshaller = Marshaller{}

// NewMarshaller creates marshaller.
func NewMarshaller() Marshaller {
	return Marshaller{}
}


// Marshaller marshals and unmarshals messages.
type Marshaller struct {
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *wire.Hello:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *wire.Hello:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *wire.Hello:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id0:
		msg := &wire.Hello{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

func size0(m *wire.Hello) uint64 {
	var n uint64 = 16
	return n
}

func marshal0(m *wire.Hello, b []byte) uint64 {
	var o uint64
	{
		// ServerID

		copy(b[o:o+16], unsafe.Slice(&m.ServerID[0], 16))
		o += 16
	}

	return o
}

func unmarshal0(m *wire.Hello, b []byte) uint64 {
	var o uint64
	{
		// ServerID

		copy(unsafe.Slice(&m.ServerID[0], 16), b[o:o+16])
		o += 16
	}

	return o
}
