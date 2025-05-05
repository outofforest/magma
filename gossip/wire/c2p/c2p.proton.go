package c2p

import (
	"reflect"

	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/types"
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
	return []any {
		Init{},
		wire.StartLogStream{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Init:
		return id1, nil
	case *wire.StartLogStream:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Init:
		return size1(msg2), nil
	case *wire.StartLogStream:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Init:
		return id1, marshal1(msg2, buf), nil
	case *wire.StartLogStream:
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
		msg := &Init{}
		return msg, unmarshal1(msg, buf), nil
	case id0:
		msg := &wire.StartLogStream{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Init:
		return id1, makePatch1(msg2, msgSrc.(*Init), buf), nil
	case *wire.StartLogStream:
		return id0, makePatch0(msg2, msgSrc.(*wire.StartLogStream), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Init:
		return applyPatch1(msg2, buf), nil
	case *wire.StartLogStream:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *wire.StartLogStream) uint64 {
	var n uint64 = 1
	{
		// Length

		helpers.UInt64Size(m.Length, &n)
	}
	return n
}

func marshal0(m *wire.StartLogStream, b []byte) uint64 {
	var o uint64
	{
		// Length

		helpers.UInt64Marshal(m.Length, b, &o)
	}

	return o
}

func unmarshal0(m *wire.StartLogStream, b []byte) uint64 {
	var o uint64
	{
		// Length

		helpers.UInt64Unmarshal(&m.Length, b, &o)
	}

	return o
}

func makePatch0(m, mSrc *wire.StartLogStream, b []byte) uint64 {
	var o uint64 = 1
	{
		// Length

		if reflect.DeepEqual(m.Length, mSrc.Length) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			helpers.UInt64Marshal(m.Length, b, &o)
		}
	}

	return o
}

func applyPatch0(m *wire.StartLogStream, b []byte) uint64 {
	var o uint64 = 1
	{
		// Length

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Length, b, &o)
		}
	}

	return o
}

func size1(m *Init) uint64 {
	var n uint64 = 2
	{
		// PartitionID

		{
			l := uint64(len(m.PartitionID))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// NextLogIndex

		helpers.UInt64Size(m.NextLogIndex, &n)
	}
	return n
}

func marshal1(m *Init, b []byte) uint64 {
	var o uint64
	{
		// PartitionID

		{
			l := uint64(len(m.PartitionID))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.PartitionID)
			o += l
		}
	}
	{
		// NextLogIndex

		helpers.UInt64Marshal(m.NextLogIndex, b, &o)
	}

	return o
}

func unmarshal1(m *Init, b []byte) uint64 {
	var o uint64
	{
		// PartitionID

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.PartitionID = types.PartitionID(b[o:o+l])
				o += l
			}
		}
	}
	{
		// NextLogIndex

		helpers.UInt64Unmarshal(&m.NextLogIndex, b, &o)
	}

	return o
}

func makePatch1(m, mSrc *Init, b []byte) uint64 {
	var o uint64 = 1
	{
		// PartitionID

		if reflect.DeepEqual(m.PartitionID, mSrc.PartitionID) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			{
				l := uint64(len(m.PartitionID))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.PartitionID)
				o += l
			}
		}
	}
	{
		// NextLogIndex

		if reflect.DeepEqual(m.NextLogIndex, mSrc.NextLogIndex) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(m.NextLogIndex, b, &o)
		}
	}

	return o
}

func applyPatch1(m *Init, b []byte) uint64 {
	var o uint64 = 1
	{
		// PartitionID

		if b[0]&0x01 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.PartitionID = types.PartitionID(b[o:o+l])
					o += l
				}
			}
		}
	}
	{
		// NextLogIndex

		if b[0]&0x02 != 0 {
			helpers.UInt64Unmarshal(&m.NextLogIndex, b, &o)
		}
	}

	return o
}
