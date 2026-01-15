package entities

import (
	"reflect"
	"time"
	"unsafe"

	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id2 uint64 = iota + 1
	id1
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
		Account{},
		Fields{},
		Blob{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Account:
		return id2, nil
	case *Fields:
		return id1, nil
	case *Blob:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Account:
		return sizei2(msg2), nil
	case *Fields:
		return sizei1(msg2), nil
	case *Blob:
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
		return id2, marshali2(msg2, buf), nil
	case *Fields:
		return id1, marshali1(msg2, buf), nil
	case *Blob:
		return id0, marshali0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id2:
		msg := &Account{}
		return msg, unmarshali2(msg, buf), nil
	case id1:
		msg := &Fields{}
		return msg, unmarshali1(msg, buf), nil
	case id0:
		msg := &Blob{}
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
		return id2, makePatchi2(msg2, msgSrc.(*Account), buf), nil
	case *Fields:
		return id1, makePatchi1(msg2, msgSrc.(*Fields), buf), nil
	case *Blob:
		return id0, makePatchi0(msg2, msgSrc.(*Blob), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverApplyPatch(&retErr)

	switch msg2 := msg.(type) {
	case *Account:
		return applyPatchi2(msg2, buf), nil
	case *Fields:
		return applyPatchi1(msg2, buf), nil
	case *Blob:
		return applyPatchi0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func sizei0(m *Blob) uint64 {
	var n uint64 = 1
	{
		// Data

		l := uint64(len(m.Data))
		helpers.UInt64Size(l, &n)
		n += l
	}
	return n
}

func marshali0(m *Blob, b []byte) uint64 {
	var o uint64
	{
		// Data

		l := uint64(len(m.Data))
		helpers.UInt64Marshal(l, b, &o)
		if l > 0 {
			copy(b[o:o+l], unsafe.Slice(&m.Data[0], l))
			o += l
		}
	}

	return o
}

func unmarshali0(m *Blob, b []byte) uint64 {
	var o uint64
	{
		// Data

		var l uint64
		helpers.UInt64Unmarshal(&l, b, &o)
		if l > 0 {
			m.Data = make([]uint8, l)
			copy(m.Data, b[o:o+l])
			o += l
		}
	}

	return o
}

func makePatchi0(m, mSrc *Blob, b []byte) uint64 {
	var o uint64 = 1
	{
		// Data

		if reflect.DeepEqual(m.Data, mSrc.Data) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			l := uint64(len(m.Data))
			helpers.UInt64Marshal(l, b, &o)
			if l > 0 {
				copy(b[o:o+l], unsafe.Slice(&m.Data[0], l))
				o += l
			}
		}
	}

	return o
}

func applyPatchi0(m *Blob, b []byte) uint64 {
	var o uint64 = 1
	{
		// Data

		if b[0]&0x01 != 0 {
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Data = make([]uint8, l)
				copy(m.Data, b[o:o+l])
				o += l
			}
		}
	}

	return o
}

func sizei1(m *Fields) uint64 {
	var n uint64 = 27
	{
		// Time

		helpers.Int64Size(m.Time.Unix() - -62135596800, &n)
		helpers.UInt32Size(uint32(m.Time.Nanosecond()), &n)
	}
	{
		// Int16

		helpers.Int16Size(m.Int16, &n)
	}
	{
		// Int32

		helpers.Int32Size(m.Int32, &n)
	}
	{
		// Int64

		helpers.Int64Size(m.Int64, &n)
	}
	{
		// Uint16

		helpers.UInt16Size(m.Uint16, &n)
	}
	{
		// Uint32

		helpers.UInt32Size(m.Uint32, &n)
	}
	{
		// Uint64

		helpers.UInt64Size(m.Uint64, &n)
	}
	return n
}

func marshali1(m *Fields, b []byte) uint64 {
	var o uint64 = 1
	{
		// Bool

		if m.Bool {
			b[0] |= 0x01
		} else {
			b[0] &= 0xFE
		}
	}
	{
		// Time

		helpers.Int64Marshal(m.Time.Unix() - -62135596800, b, &o)
		helpers.UInt32Marshal(uint32(m.Time.Nanosecond()), b, &o)
	}
	{
		// Int8

		b[o] = byte(m.Int8)
		o++
	}
	{
		// Int16

		helpers.Int16Marshal(m.Int16, b, &o)
	}
	{
		// Int32

		helpers.Int32Marshal(m.Int32, b, &o)
	}
	{
		// Int64

		helpers.Int64Marshal(m.Int64, b, &o)
	}
	{
		// Uint8

		b[o] = m.Uint8
		o++
	}
	{
		// Uint16

		helpers.UInt16Marshal(m.Uint16, b, &o)
	}
	{
		// Uint32

		helpers.UInt32Marshal(m.Uint32, b, &o)
	}
	{
		// Uint64

		helpers.UInt64Marshal(m.Uint64, b, &o)
	}
	{
		// EntityID

		copy(b[o:o+16], unsafe.Slice(&m.EntityID[0], 16))
		o += 16
	}

	return o
}

func unmarshali1(m *Fields, b []byte) uint64 {
	var o uint64 = 1
	{
		// Bool

		m.Bool = b[0]&0x01 != 0
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
		// Int8

		m.Int8 = int8(b[o])
		o++
	}
	{
		// Int16

		helpers.Int16Unmarshal(&m.Int16, b, &o)
	}
	{
		// Int32

		helpers.Int32Unmarshal(&m.Int32, b, &o)
	}
	{
		// Int64

		helpers.Int64Unmarshal(&m.Int64, b, &o)
	}
	{
		// Uint8

		m.Uint8 = b[o]
		o++
	}
	{
		// Uint16

		helpers.UInt16Unmarshal(&m.Uint16, b, &o)
	}
	{
		// Uint32

		helpers.UInt32Unmarshal(&m.Uint32, b, &o)
	}
	{
		// Uint64

		helpers.UInt64Unmarshal(&m.Uint64, b, &o)
	}
	{
		// EntityID

		copy(unsafe.Slice(&m.EntityID[0], 16), b[o:o+16])
		o += 16
	}

	return o
}

func makePatchi1(m, mSrc *Fields, b []byte) uint64 {
	var o uint64 = 3
	{
		// Bool

		if m.Bool == mSrc.Bool {
			b[2] &= 0xFE
		} else {
			b[2] |= 0x01
		}
	}
	{
		// Time

		if reflect.DeepEqual(m.Time, mSrc.Time) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			helpers.Int64Marshal(m.Time.Unix() - -62135596800, b, &o)
			helpers.UInt32Marshal(uint32(m.Time.Nanosecond()), b, &o)
		}
	}
	{
		// Int8

		if reflect.DeepEqual(m.Int8, mSrc.Int8) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			b[o] = byte(m.Int8)
			o++
		}
	}
	{
		// Int16

		if reflect.DeepEqual(m.Int16, mSrc.Int16) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			helpers.Int16Marshal(m.Int16, b, &o)
		}
	}
	{
		// Int32

		if reflect.DeepEqual(m.Int32, mSrc.Int32) {
			b[0] &= 0xF7
		} else {
			b[0] |= 0x08
			helpers.Int32Marshal(m.Int32, b, &o)
		}
	}
	{
		// Int64

		if reflect.DeepEqual(m.Int64, mSrc.Int64) {
			b[0] &= 0xEF
		} else {
			b[0] |= 0x10
			helpers.Int64Marshal(m.Int64, b, &o)
		}
	}
	{
		// Uint8

		if reflect.DeepEqual(m.Uint8, mSrc.Uint8) {
			b[0] &= 0xDF
		} else {
			b[0] |= 0x20
			b[o] = m.Uint8
			o++
		}
	}
	{
		// Uint16

		if reflect.DeepEqual(m.Uint16, mSrc.Uint16) {
			b[0] &= 0xBF
		} else {
			b[0] |= 0x40
			helpers.UInt16Marshal(m.Uint16, b, &o)
		}
	}
	{
		// Uint32

		if reflect.DeepEqual(m.Uint32, mSrc.Uint32) {
			b[0] &= 0x7F
		} else {
			b[0] |= 0x80
			helpers.UInt32Marshal(m.Uint32, b, &o)
		}
	}
	{
		// Uint64

		if reflect.DeepEqual(m.Uint64, mSrc.Uint64) {
			b[1] &= 0xFE
		} else {
			b[1] |= 0x01
			helpers.UInt64Marshal(m.Uint64, b, &o)
		}
	}
	{
		// EntityID

		if reflect.DeepEqual(m.EntityID, mSrc.EntityID) {
			b[1] &= 0xFD
		} else {
			b[1] |= 0x02
			copy(b[o:o+16], unsafe.Slice(&m.EntityID[0], 16))
			o += 16
		}
	}

	return o
}

func applyPatchi1(m *Fields, b []byte) uint64 {
	var o uint64 = 3
	{
		// Bool

		if b[2]&0x01 != 0 {
			m.Bool = !m.Bool
		}
	}
	{
		// Time

		if b[0]&0x01 != 0 {
			var seconds int64
			var nanoseconds uint32
			helpers.Int64Unmarshal(&seconds, b, &o)
			helpers.UInt32Unmarshal(&nanoseconds, b, &o)
			m.Time = time.Unix(seconds + -62135596800, int64(nanoseconds))
		}
	}
	{
		// Int8

		if b[0]&0x02 != 0 {
			m.Int8 = int8(b[o])
			o++
		}
	}
	{
		// Int16

		if b[0]&0x04 != 0 {
			helpers.Int16Unmarshal(&m.Int16, b, &o)
		}
	}
	{
		// Int32

		if b[0]&0x08 != 0 {
			helpers.Int32Unmarshal(&m.Int32, b, &o)
		}
	}
	{
		// Int64

		if b[0]&0x10 != 0 {
			helpers.Int64Unmarshal(&m.Int64, b, &o)
		}
	}
	{
		// Uint8

		if b[0]&0x20 != 0 {
			m.Uint8 = b[o]
			o++
		}
	}
	{
		// Uint16

		if b[0]&0x40 != 0 {
			helpers.UInt16Unmarshal(&m.Uint16, b, &o)
		}
	}
	{
		// Uint32

		if b[0]&0x80 != 0 {
			helpers.UInt32Unmarshal(&m.Uint32, b, &o)
		}
	}
	{
		// Uint64

		if b[1]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Uint64, b, &o)
		}
	}
	{
		// EntityID

		if b[1]&0x02 != 0 {
			copy(unsafe.Slice(&m.EntityID[0], 16), b[o:o+16])
			o += 16
		}
	}

	return o
}

func sizei2(m *Account) uint64 {
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

func marshali2(m *Account, b []byte) uint64 {
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

func unmarshali2(m *Account, b []byte) uint64 {
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

func makePatchi2(m, mSrc *Account, b []byte) uint64 {
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

func applyPatchi2(m *Account, b []byte) uint64 {
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
