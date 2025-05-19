package l2p

import (
	"reflect"

	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id3 uint64 = iota + 1
	id2
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
		types.LogSyncRequest{},
		types.LogSyncResponse{},
		wire.StartLogStream{},
		wire.HotEnd{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *types.LogSyncRequest:
		return id3, nil
	case *types.LogSyncResponse:
		return id2, nil
	case *wire.StartLogStream:
		return id1, nil
	case *wire.HotEnd:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *types.LogSyncRequest:
		return size3(msg2), nil
	case *types.LogSyncResponse:
		return size2(msg2), nil
	case *wire.StartLogStream:
		return size1(msg2), nil
	case *wire.HotEnd:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *types.LogSyncRequest:
		return id3, marshal3(msg2, buf), nil
	case *types.LogSyncResponse:
		return id2, marshal2(msg2, buf), nil
	case *wire.StartLogStream:
		return id1, marshal1(msg2, buf), nil
	case *wire.HotEnd:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id3:
		msg := &types.LogSyncRequest{}
		return msg, unmarshal3(msg, buf), nil
	case id2:
		msg := &types.LogSyncResponse{}
		return msg, unmarshal2(msg, buf), nil
	case id1:
		msg := &wire.StartLogStream{}
		return msg, unmarshal1(msg, buf), nil
	case id0:
		msg := &wire.HotEnd{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *types.LogSyncRequest:
		return id3, makePatch3(msg2, msgSrc.(*types.LogSyncRequest), buf), nil
	case *types.LogSyncResponse:
		return id2, makePatch2(msg2, msgSrc.(*types.LogSyncResponse), buf), nil
	case *wire.StartLogStream:
		return id1, makePatch1(msg2, msgSrc.(*wire.StartLogStream), buf), nil
	case *wire.HotEnd:
		return id0, makePatch0(msg2, msgSrc.(*wire.HotEnd), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *types.LogSyncRequest:
		return applyPatch3(msg2, buf), nil
	case *types.LogSyncResponse:
		return applyPatch2(msg2, buf), nil
	case *wire.StartLogStream:
		return applyPatch1(msg2, buf), nil
	case *wire.HotEnd:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *wire.HotEnd) uint64 {
	var n uint64
	return n
}

func marshal0(m *wire.HotEnd, b []byte) uint64 {
	var o uint64

	return o
}

func unmarshal0(m *wire.HotEnd, b []byte) uint64 {
	var o uint64

	return o
}

func makePatch0(m, mSrc *wire.HotEnd, b []byte) uint64 {
	var o uint64

	return o
}

func applyPatch0(m *wire.HotEnd, b []byte) uint64 {
	var o uint64

	return o
}

func size1(m *wire.StartLogStream) uint64 {
	var n uint64 = 1
	{
		// Length

		helpers.UInt64Size(m.Length, &n)
	}
	return n
}

func marshal1(m *wire.StartLogStream, b []byte) uint64 {
	var o uint64
	{
		// Length

		helpers.UInt64Marshal(m.Length, b, &o)
	}

	return o
}

func unmarshal1(m *wire.StartLogStream, b []byte) uint64 {
	var o uint64
	{
		// Length

		helpers.UInt64Unmarshal(&m.Length, b, &o)
	}

	return o
}

func makePatch1(m, mSrc *wire.StartLogStream, b []byte) uint64 {
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

func applyPatch1(m *wire.StartLogStream, b []byte) uint64 {
	var o uint64 = 1
	{
		// Length

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Length, b, &o)
		}
	}

	return o
}

func size2(m *types.LogSyncResponse) uint64 {
	var n uint64 = 3
	{
		// Term

		helpers.UInt64Size(m.Term, &n)
	}
	{
		// NextIndex

		helpers.UInt64Size(m.NextIndex, &n)
	}
	{
		// SyncIndex

		helpers.UInt64Size(m.SyncIndex, &n)
	}
	return n
}

func marshal2(m *types.LogSyncResponse, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Marshal(m.Term, b, &o)
	}
	{
		// NextIndex

		helpers.UInt64Marshal(m.NextIndex, b, &o)
	}
	{
		// SyncIndex

		helpers.UInt64Marshal(m.SyncIndex, b, &o)
	}

	return o
}

func unmarshal2(m *types.LogSyncResponse, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Unmarshal(&m.Term, b, &o)
	}
	{
		// NextIndex

		helpers.UInt64Unmarshal(&m.NextIndex, b, &o)
	}
	{
		// SyncIndex

		helpers.UInt64Unmarshal(&m.SyncIndex, b, &o)
	}

	return o
}

func makePatch2(m, mSrc *types.LogSyncResponse, b []byte) uint64 {
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
	{
		// NextIndex

		if reflect.DeepEqual(m.NextIndex, mSrc.NextIndex) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(m.NextIndex, b, &o)
		}
	}
	{
		// SyncIndex

		if reflect.DeepEqual(m.SyncIndex, mSrc.SyncIndex) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			helpers.UInt64Marshal(m.SyncIndex, b, &o)
		}
	}

	return o
}

func applyPatch2(m *types.LogSyncResponse, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Term, b, &o)
		}
	}
	{
		// NextIndex

		if b[0]&0x02 != 0 {
			helpers.UInt64Unmarshal(&m.NextIndex, b, &o)
		}
	}
	{
		// SyncIndex

		if b[0]&0x04 != 0 {
			helpers.UInt64Unmarshal(&m.SyncIndex, b, &o)
		}
	}

	return o
}

func size3(m *types.LogSyncRequest) uint64 {
	var n uint64 = 3
	{
		// Term

		helpers.UInt64Size(m.Term, &n)
	}
	{
		// NextIndex

		helpers.UInt64Size(m.NextIndex, &n)
	}
	{
		// LastTerm

		helpers.UInt64Size(m.LastTerm, &n)
	}
	return n
}

func marshal3(m *types.LogSyncRequest, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Marshal(m.Term, b, &o)
	}
	{
		// NextIndex

		helpers.UInt64Marshal(m.NextIndex, b, &o)
	}
	{
		// LastTerm

		helpers.UInt64Marshal(m.LastTerm, b, &o)
	}

	return o
}

func unmarshal3(m *types.LogSyncRequest, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Unmarshal(&m.Term, b, &o)
	}
	{
		// NextIndex

		helpers.UInt64Unmarshal(&m.NextIndex, b, &o)
	}
	{
		// LastTerm

		helpers.UInt64Unmarshal(&m.LastTerm, b, &o)
	}

	return o
}

func makePatch3(m, mSrc *types.LogSyncRequest, b []byte) uint64 {
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
	{
		// NextIndex

		if reflect.DeepEqual(m.NextIndex, mSrc.NextIndex) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(m.NextIndex, b, &o)
		}
	}
	{
		// LastTerm

		if reflect.DeepEqual(m.LastTerm, mSrc.LastTerm) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			helpers.UInt64Marshal(m.LastTerm, b, &o)
		}
	}

	return o
}

func applyPatch3(m *types.LogSyncRequest, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Term, b, &o)
		}
	}
	{
		// NextIndex

		if b[0]&0x02 != 0 {
			helpers.UInt64Unmarshal(&m.NextIndex, b, &o)
		}
	}
	{
		// LastTerm

		if b[0]&0x04 != 0 {
			helpers.UInt64Unmarshal(&m.LastTerm, b, &o)
		}
	}

	return o
}
