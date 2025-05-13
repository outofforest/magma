package entities

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
	return []any {
		Account{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Account:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Account:
		return sizei0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Account:
		return id0, marshali0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id0:
		msg := &Account{}
		return msg, unmarshali0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Account:
		return id0, makePatchi0(msg2, msgSrc.(*Account), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Account:
		return applyPatchi0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func sizei0(m *Account) uint64 {
	var n uint64 = 2
	{
		// FirstName

		{
			l := uint64(len(m.FirstName))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// LastName

		{
			l := uint64(len(m.LastName))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshali0(m *Account, b []byte) uint64 {
	var o uint64
	{
		// FirstName

		{
			l := uint64(len(m.FirstName))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.FirstName)
			o += l
		}
	}
	{
		// LastName

		{
			l := uint64(len(m.LastName))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.LastName)
			o += l
		}
	}

	return o
}

func unmarshali0(m *Account, b []byte) uint64 {
	var o uint64
	{
		// FirstName

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.FirstName = string(b[o:o+l])
				o += l
			}
		}
	}
	{
		// LastName

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.LastName = string(b[o:o+l])
				o += l
			}
		}
	}

	return o
}

func makePatchi0(m, mSrc *Account, b []byte) uint64 {
	var o uint64 = 1
	{
		// FirstName

		if reflect.DeepEqual(m.FirstName, mSrc.FirstName) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			{
				l := uint64(len(m.FirstName))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.FirstName)
				o += l
			}
		}
	}
	{
		// LastName

		if reflect.DeepEqual(m.LastName, mSrc.LastName) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			{
				l := uint64(len(m.LastName))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.LastName)
				o += l
			}
		}
	}

	return o
}

func applyPatchi0(m *Account, b []byte) uint64 {
	var o uint64 = 1
	{
		// FirstName

		if b[0]&0x01 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.FirstName = string(b[o:o+l])
					o += l
				}
			}
		}
	}
	{
		// LastName

		if b[0]&0x02 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.LastName = string(b[o:o+l])
					o += l
				}
			}
		}
	}

	return o
}
