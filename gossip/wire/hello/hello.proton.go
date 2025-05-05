package hello

import (
	"reflect"

	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/types"
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
	return []any{
		wire.Hello{},
	}
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

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *wire.Hello:
		return id0, makePatch0(msg2, msgSrc.(*wire.Hello), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *wire.Hello:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *wire.Hello) uint64 {
	var n uint64 = 3
	{
		// ServerID

		{
			l := uint64(len(m.ServerID))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// PartitionID

		{
			l := uint64(len(m.PartitionID))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal0(m *wire.Hello, b []byte) uint64 {
	var o uint64
	{
		// ServerID

		{
			l := uint64(len(m.ServerID))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.ServerID)
			o += l
		}
	}
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
		// Channel

		b[o] = byte(m.Channel)
		o++
	}

	return o
}

func unmarshal0(m *wire.Hello, b []byte) uint64 {
	var o uint64
	{
		// ServerID

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.ServerID = types.ServerID(b[o : o+l])
				o += l
			}
		}
	}
	{
		// PartitionID

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.PartitionID = types.PartitionID(b[o : o+l])
				o += l
			}
		}
	}
	{
		// Channel

		m.Channel = wire.Channel(b[o])
		o++
	}

	return o
}

func makePatch0(m, mSrc *wire.Hello, b []byte) uint64 {
	var o uint64 = 1
	{
		// ServerID

		if reflect.DeepEqual(m.ServerID, mSrc.ServerID) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			{
				l := uint64(len(m.ServerID))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.ServerID)
				o += l
			}
		}
	}
	{
		// PartitionID

		if reflect.DeepEqual(m.PartitionID, mSrc.PartitionID) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			{
				l := uint64(len(m.PartitionID))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.PartitionID)
				o += l
			}
		}
	}
	{
		// Channel

		if reflect.DeepEqual(m.Channel, mSrc.Channel) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			b[o] = byte(m.Channel)
			o++
		}
	}

	return o
}

func applyPatch0(m *wire.Hello, b []byte) uint64 {
	var o uint64 = 1
	{
		// ServerID

		if b[0]&0x01 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.ServerID = types.ServerID(b[o : o+l])
					o += l
				}
			}
		}
	}
	{
		// PartitionID

		if b[0]&0x02 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.PartitionID = types.PartitionID(b[o : o+l])
					o += l
				}
			}
		}
	}
	{
		// Channel

		if b[0]&0x04 != 0 {
			m.Channel = wire.Channel(b[o])
			o++
		}
	}

	return o
}
