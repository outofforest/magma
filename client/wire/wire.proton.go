package wire

import (
	"reflect"
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
	return []any {
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

// IsPatchNeeded checks if non-empty patch exists.
func (m Marshaller) IsPatchNeeded(msgDst, msgSrc any) (bool, error) {
	switch msg2 := msgDst.(type) {
	case *TxMetadata:
		return isPatchNeeded1(msg2, msgSrc.(*TxMetadata)), nil
	case *EntityMetadata:
		return isPatchNeeded0(msg2, msgSrc.(*EntityMetadata)), nil
	default:
		return false, errors.Errorf("unknown message type %T", msgDst)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *TxMetadata:
		return id1, makePatch1(msg2, msgSrc.(*TxMetadata), buf), nil
	case *EntityMetadata:
		return id0, makePatch0(msg2, msgSrc.(*EntityMetadata), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverApplyPatch(&retErr)

	switch msg2 := msg.(type) {
	case *TxMetadata:
		return applyPatch1(msg2, buf), nil
	case *EntityMetadata:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
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

func isPatchNeeded0(m, mSrc *EntityMetadata) bool {
	{
		// ID

		if !reflect.DeepEqual(m.ID, mSrc.ID) {
			return true
		}

	}
	{
		// Revision

		if !reflect.DeepEqual(m.Revision, mSrc.Revision) {
			return true
		}

	}
	{
		// MessageID

		if !reflect.DeepEqual(m.MessageID, mSrc.MessageID) {
			return true
		}

	}

	return false
}

func makePatch0(m, mSrc *EntityMetadata, b []byte) uint64 {
	var o uint64 = 1
	{
		// ID

		if reflect.DeepEqual(m.ID, mSrc.ID) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			copy(b[o:o+16], unsafe.Slice(&m.ID[0], 16))
			o += 16
		}
	}
	{
		// Revision

		if reflect.DeepEqual(m.Revision, mSrc.Revision) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(m.Revision, b, &o)
		}
	}
	{
		// MessageID

		if reflect.DeepEqual(m.MessageID, mSrc.MessageID) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			helpers.UInt64Marshal(m.MessageID, b, &o)
		}
	}

	return o
}

func applyPatch0(m *EntityMetadata, b []byte) uint64 {
	var o uint64 = 1
	{
		// ID

		if b[0]&0x01 != 0 {
			copy(unsafe.Slice(&m.ID[0], 16), b[o:o+16])
			o += 16
		}
	}
	{
		// Revision

		if b[0]&0x02 != 0 {
			helpers.UInt64Unmarshal(&m.Revision, b, &o)
		}
	}
	{
		// MessageID

		if b[0]&0x04 != 0 {
			helpers.UInt64Unmarshal(&m.MessageID, b, &o)
		}
	}

	return o
}

func size1(m *TxMetadata) uint64 {
	var n uint64 = 20
	{
		// Time

		helpers.Int64Size(m.Time.Unix() - -62135596800, &n)
		helpers.UInt32Size(uint32(m.Time.Nanosecond()), &n)
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

		helpers.Int64Marshal(m.Time.Unix() - -62135596800, b, &o)
		helpers.UInt32Marshal(uint32(m.Time.Nanosecond()), b, &o)
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

		var seconds int64
		var nanoseconds uint32
		helpers.Int64Unmarshal(&seconds, b, &o)
		helpers.UInt32Unmarshal(&nanoseconds, b, &o)
		m.Time = time.Unix(seconds + -62135596800, int64(nanoseconds))
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
	{
		// EntityMetadataID

		helpers.UInt64Unmarshal(&m.EntityMetadataID, b, &o)
	}

	return o
}

func isPatchNeeded1(m, mSrc *TxMetadata) bool {
	{
		// ID

		if !reflect.DeepEqual(m.ID, mSrc.ID) {
			return true
		}

	}
	{
		// Time

		if !reflect.DeepEqual(m.Time, mSrc.Time) {
			return true
		}

	}
	{
		// Service

		if !reflect.DeepEqual(m.Service, mSrc.Service) {
			return true
		}

	}
	{
		// EntityMetadataID

		if !reflect.DeepEqual(m.EntityMetadataID, mSrc.EntityMetadataID) {
			return true
		}

	}

	return false
}

func makePatch1(m, mSrc *TxMetadata, b []byte) uint64 {
	var o uint64 = 1
	{
		// ID

		if reflect.DeepEqual(m.ID, mSrc.ID) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			copy(b[o:o+16], unsafe.Slice(&m.ID[0], 16))
			o += 16
		}
	}
	{
		// Time

		if reflect.DeepEqual(m.Time, mSrc.Time) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.Int64Marshal(m.Time.Unix() - -62135596800, b, &o)
			helpers.UInt32Marshal(uint32(m.Time.Nanosecond()), b, &o)
		}
	}
	{
		// Service

		if reflect.DeepEqual(m.Service, mSrc.Service) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			{
				l := uint64(len(m.Service))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.Service)
				o += l
			}
		}
	}
	{
		// EntityMetadataID

		if reflect.DeepEqual(m.EntityMetadataID, mSrc.EntityMetadataID) {
			b[0] &= 0xF7
		} else {
			b[0] |= 0x08
			helpers.UInt64Marshal(m.EntityMetadataID, b, &o)
		}
	}

	return o
}

func applyPatch1(m *TxMetadata, b []byte) uint64 {
	var o uint64 = 1
	{
		// ID

		if b[0]&0x01 != 0 {
			copy(unsafe.Slice(&m.ID[0], 16), b[o:o+16])
			o += 16
		}
	}
	{
		// Time

		if b[0]&0x02 != 0 {
			var seconds int64
			var nanoseconds uint32
			helpers.Int64Unmarshal(&seconds, b, &o)
			helpers.UInt32Unmarshal(&nanoseconds, b, &o)
			m.Time = time.Unix(seconds + -62135596800, int64(nanoseconds))
		}
	}
	{
		// Service

		if b[0]&0x04 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.Service = string(b[o:o+l])
					o += l
				}
			}
		}
	}
	{
		// EntityMetadataID

		if b[0]&0x08 != 0 {
			helpers.UInt64Unmarshal(&m.EntityMetadataID, b, &o)
		}
	}

	return o
}
