package wire

import (
	"time"
	"unsafe"

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

// Messages returns list of the message types supported by marshaller.
func (m Marshaller) Messages() []any {
	return []any {
		TxMetadata{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *TxMetadata:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *TxMetadata:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *TxMetadata:
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
		msg := &TxMetadata{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

func size0(m *TxMetadata) uint64 {
	var n uint64 = 18
	{
		// Time

		helpers.Int64Size(m.Time.Unix(), &n)
	}
	{
		// Service

		{
			l := uint64(len(m.Service))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal0(m *TxMetadata, b []byte) uint64 {
	var o uint64
	{
		// ID

		copy(b[o:o+16], unsafe.Slice(&m.ID[0], 16))
		o += 16
	}
	{
		// Time

		helpers.Int64Marshal(m.Time.Unix(), b, &o)
	}
	{
		// Service

		{
			l := uint64(len(m.Service))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Service)
			o += l
		}
	}

	return o
}

func unmarshal0(m *TxMetadata, b []byte) uint64 {
	var o uint64
	{
		// ID

		copy(unsafe.Slice(&m.ID[0], 16), b[o:o+16])
		o += 16
	}
	{
		// Time

		var vi int64
		helpers.Int64Unmarshal(&vi, b, &o)
		m.Time = time.Unix(vi, 0)
	}
	{
		// Service

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Service = string(b[o:o+l])
				o += l
			}
		}
	}

	return o
}
