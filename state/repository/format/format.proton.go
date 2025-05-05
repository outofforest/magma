package format

import (
	"reflect"

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
		Header{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Header:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Header:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Header:
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
		msg := &Header{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Header:
		return id0, makePatch0(msg2, msgSrc.(*Header), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Header:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *Header) uint64 {
	var n uint64 = 5
	{
		// PreviousTerm

		helpers.UInt64Size(m.PreviousTerm, &n)
	}
	{
		// PreviousChecksum

		helpers.UInt64Size(m.PreviousChecksum, &n)
	}
	{
		// Term

		helpers.UInt64Size(m.Term, &n)
	}
	{
		// NextLogIndex

		helpers.UInt64Size(m.NextLogIndex, &n)
	}
	{
		// HeaderChecksum

		helpers.UInt64Size(m.HeaderChecksum, &n)
	}
	return n
}

func marshal0(m *Header, b []byte) uint64 {
	var o uint64
	{
		// PreviousTerm

		helpers.UInt64Marshal(m.PreviousTerm, b, &o)
	}
	{
		// PreviousChecksum

		helpers.UInt64Marshal(m.PreviousChecksum, b, &o)
	}
	{
		// Term

		helpers.UInt64Marshal(m.Term, b, &o)
	}
	{
		// NextLogIndex

		helpers.UInt64Marshal(m.NextLogIndex, b, &o)
	}
	{
		// HeaderChecksum

		helpers.UInt64Marshal(m.HeaderChecksum, b, &o)
	}

	return o
}

func unmarshal0(m *Header, b []byte) uint64 {
	var o uint64
	{
		// PreviousTerm

		helpers.UInt64Unmarshal(&m.PreviousTerm, b, &o)
	}
	{
		// PreviousChecksum

		helpers.UInt64Unmarshal(&m.PreviousChecksum, b, &o)
	}
	{
		// Term

		helpers.UInt64Unmarshal(&m.Term, b, &o)
	}
	{
		// NextLogIndex

		helpers.UInt64Unmarshal(&m.NextLogIndex, b, &o)
	}
	{
		// HeaderChecksum

		helpers.UInt64Unmarshal(&m.HeaderChecksum, b, &o)
	}

	return o
}

func makePatch0(m, mSrc *Header, b []byte) uint64 {
	var o uint64 = 1
	{
		// PreviousTerm

		if reflect.DeepEqual(m.PreviousTerm, mSrc.PreviousTerm) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			helpers.UInt64Marshal(m.PreviousTerm, b, &o)
		}
	}
	{
		// PreviousChecksum

		if reflect.DeepEqual(m.PreviousChecksum, mSrc.PreviousChecksum) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(m.PreviousChecksum, b, &o)
		}
	}
	{
		// Term

		if reflect.DeepEqual(m.Term, mSrc.Term) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			helpers.UInt64Marshal(m.Term, b, &o)
		}
	}
	{
		// NextLogIndex

		if reflect.DeepEqual(m.NextLogIndex, mSrc.NextLogIndex) {
			b[0] &= 0xF7
		} else {
			b[0] |= 0x08
			helpers.UInt64Marshal(m.NextLogIndex, b, &o)
		}
	}
	{
		// HeaderChecksum

		if reflect.DeepEqual(m.HeaderChecksum, mSrc.HeaderChecksum) {
			b[0] &= 0xEF
		} else {
			b[0] |= 0x10
			helpers.UInt64Marshal(m.HeaderChecksum, b, &o)
		}
	}

	return o
}

func applyPatch0(m *Header, b []byte) uint64 {
	var o uint64 = 1
	{
		// PreviousTerm

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.PreviousTerm, b, &o)
		}
	}
	{
		// PreviousChecksum

		if b[0]&0x02 != 0 {
			helpers.UInt64Unmarshal(&m.PreviousChecksum, b, &o)
		}
	}
	{
		// Term

		if b[0]&0x04 != 0 {
			helpers.UInt64Unmarshal(&m.Term, b, &o)
		}
	}
	{
		// NextLogIndex

		if b[0]&0x08 != 0 {
			helpers.UInt64Unmarshal(&m.NextLogIndex, b, &o)
		}
	}
	{
		// HeaderChecksum

		if b[0]&0x10 != 0 {
			helpers.UInt64Unmarshal(&m.HeaderChecksum, b, &o)
		}
	}

	return o
}
