package wire

import (
	"time"
	"unsafe"

	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id1 uint64 = iota + 1
	id0
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
	return []any{
		TxMetadata{},
		EntityMetadata{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *TxMetadata:
		return id1, nil
	case *EntityMetadata:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *TxMetadata:
		return size1(msg2), nil
	case *EntityMetadata:
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
		return id1, marshal1(msg2, buf), nil
	case *EntityMetadata:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id1:
		msg := &TxMetadata{}
		return msg, unmarshal1(msg, buf), nil
	case id0:
		msg := &EntityMetadata{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

func size0(m *EntityMetadata) uint64 {
	var n uint64 = 18
	{
		// Revision

		helpers.UInt64Size(m.Revision, &n)
	}
	{
		// MessageID

		helpers.UInt64Size(m.MessageID, &n)
	}
	return n
}

func marshal0(m *EntityMetadata, b []byte) uint64 {
	var o uint64
	{
		// ID

		copy(b[o:o+16], unsafe.Slice(&m.ID[0], 16))
		o += 16
	}
	{
		// Revision

		helpers.UInt64Marshal(m.Revision, b, &o)
	}
	{
		// MessageID

		helpers.UInt64Marshal(m.MessageID, b, &o)
	}

	return o
}

func unmarshal0(m *EntityMetadata, b []byte) uint64 {
	var o uint64
	{
		// ID

		copy(unsafe.Slice(&m.ID[0], 16), b[o:o+16])
		o += 16
	}
	{
		// Revision

		helpers.UInt64Unmarshal(&m.Revision, b, &o)
	}
	{
		// MessageID

		helpers.UInt64Unmarshal(&m.MessageID, b, &o)
	}

	return o
}

func size1(m *TxMetadata) uint64 {
	var n uint64 = 19
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
	{
		// EntityMetadataID

		helpers.UInt64Size(m.EntityMetadataID, &n)
	}
	return n
}

func marshal1(m *TxMetadata, b []byte) uint64 {
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
	{
		// EntityMetadataID

		helpers.UInt64Marshal(m.EntityMetadataID, b, &o)
	}

	return o
}

func unmarshal1(m *TxMetadata, b []byte) uint64 {
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
				m.Service = string(b[o : o+l])
				o += l
			}
		}
	}
	{
		// EntityMetadataID

		helpers.UInt64Unmarshal(&m.EntityMetadataID, b, &o)
	}

	return o
}
