package format

import (
	"reflect"

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
		Term{},
		Vote{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Term:
		return id1, nil
	case *Vote:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Term:
		return size1(msg2), nil
	case *Vote:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Term:
		return id1, marshal1(msg2, buf), nil
	case *Vote:
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
		msg := &Term{}
		return msg, unmarshal1(msg, buf), nil
	case id0:
		msg := &Vote{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// IsPatchNeeded checks if non-empty patch exists.
func (m Marshaller) IsPatchNeeded(msgDst, msgSrc any) (bool, error) {
	switch msg2 := msgDst.(type) {
	case *Term:
		return isPatchNeeded1(msg2, msgSrc.(*Term)), nil
	case *Vote:
		return isPatchNeeded0(msg2, msgSrc.(*Vote)), nil
	default:
		return false, errors.Errorf("unknown message type %T", msgDst)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Term:
		return id1, makePatch1(msg2, msgSrc.(*Term), buf), nil
	case *Vote:
		return id0, makePatch0(msg2, msgSrc.(*Vote), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverApplyPatch(&retErr)

	switch msg2 := msg.(type) {
	case *Term:
		return applyPatch1(msg2, buf), nil
	case *Vote:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *Vote) uint64 {
	var n uint64 = 1
	{
		// Candidate

		{
			l := uint64(len(m.Candidate))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal0(m *Vote, b []byte) uint64 {
	var o uint64
	{
		// Candidate

		{
			l := uint64(len(m.Candidate))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Candidate)
			o += l
		}
	}

	return o
}

func unmarshal0(m *Vote, b []byte) uint64 {
	var o uint64
	{
		// Candidate

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Candidate = types.ServerID(b[o:o+l])
				o += l
			}
		}
	}

	return o
}

func isPatchNeeded0(m, mSrc *Vote) bool {
	{
		// Candidate

		if !reflect.DeepEqual(m.Candidate, mSrc.Candidate) {
			return true
		}

	}

	return false
}

func makePatch0(m, mSrc *Vote, b []byte) uint64 {
	var o uint64 = 1
	{
		// Candidate

		if reflect.DeepEqual(m.Candidate, mSrc.Candidate) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			{
				l := uint64(len(m.Candidate))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.Candidate)
				o += l
			}
		}
	}

	return o
}

func applyPatch0(m *Vote, b []byte) uint64 {
	var o uint64 = 1
	{
		// Candidate

		if b[0]&0x01 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.Candidate = types.ServerID(b[o:o+l])
					o += l
				}
			}
		}
	}

	return o
}

func size1(m *Term) uint64 {
	var n uint64 = 1
	{
		// Term

		helpers.UInt64Size(m.Term, &n)
	}
	return n
}

func marshal1(m *Term, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Marshal(m.Term, b, &o)
	}

	return o
}

func unmarshal1(m *Term, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Unmarshal(&m.Term, b, &o)
	}

	return o
}

func isPatchNeeded1(m, mSrc *Term) bool {
	{
		// Term

		if !reflect.DeepEqual(m.Term, mSrc.Term) {
			return true
		}

	}

	return false
}

func makePatch1(m, mSrc *Term, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		if reflect.DeepEqual(m.Term, mSrc.Term) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			helpers.UInt64Marshal(m.Term, b, &o)
		}
	}

	return o
}

func applyPatch1(m *Term, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Term, b, &o)
		}
	}

	return o
}
