package p2c

import (
	"unsafe"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
	"github.com/pkg/errors"
)

const (
	id0 uint64 = iota + 1
)

var _ proton.Marshaller = Marshaller{}

// NewMarshaller creates marshaller.
func NewMarshaller() Marshaller {
	return Marshaller{
	}
}

// Marshaller marshals and unmarshals messages.
type Marshaller struct {
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *types.ClientRequest:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *types.ClientRequest:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer func() {
		if res := recover(); res != nil {
			retErr = errors.Errorf("marshaling message failed: %s", res)
		}
	}()

	switch msg2 := msg.(type) {
	case *types.ClientRequest:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer func() {
		if res := recover(); res != nil {
			retErr = errors.Errorf("unmarshaling message failed: %s", res)
		}
	}()

	switch id {
	case id0:
		msg := &types.ClientRequest{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

func size0(m *types.ClientRequest) uint64 {
	var n uint64 = 1
	{
		// Data

		l := uint64(len(m.Data))
		{
			vi := l
			switch {
			case vi <= 0x7F:
			case vi <= 0x3FFF:
				n++
			case vi <= 0x1FFFFF:
				n += 2
			case vi <= 0xFFFFFFF:
				n += 3
			case vi <= 0x7FFFFFFFF:
				n += 4
			case vi <= 0x3FFFFFFFFFF:
				n += 5
			case vi <= 0x1FFFFFFFFFFFF:
				n += 6
			case vi <= 0xFFFFFFFFFFFFFF:
				n += 7
			default:
				n += 8
			}
		}
		n += l
	}
	return n
}

func marshal0(m *types.ClientRequest, b []byte) uint64 {
	var o uint64
	{
		// Data

		l := uint64(len(m.Data))
		{
			vi := l
			switch {
			case vi <= 0x7F:
				b[o] = byte(vi)
				o++
			case vi <= 0x3FFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			case vi <= 0x1FFFFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			case vi <= 0xFFFFFFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			case vi <= 0x7FFFFFFFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			case vi <= 0x3FFFFFFFFFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			case vi <= 0x1FFFFFFFFFFFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			case vi <= 0xFFFFFFFFFFFFFF:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			default:
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi) | 0x80
				o++
				vi >>= 7
				b[o] = byte(vi)
				o++
			}
		}
		if l > 0 {
			copy(b[o:o+l], unsafe.Slice(&m.Data[0], l))
			o += l
		}
	}

	return o
}

func unmarshal0(m *types.ClientRequest, b []byte) uint64 {
	var o uint64
	{
		// Data

		var l uint64
		{
			vi := uint64(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= uint64(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= uint64(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= uint64(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= uint64(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= uint64(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= uint64(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= uint64(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= uint64(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			l = vi
		}
		if l > 0 {
			m.Data = make([]uint8, l)
			copy(m.Data, b[o:o+l])
			o += l
		}
	}

	return o
}
