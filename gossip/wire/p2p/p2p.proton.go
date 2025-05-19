package p2p

import (
	"reflect"

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
		types.LogACK{},
		types.VoteRequest{},
		types.VoteResponse{},
		types.Heartbeat{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *types.LogACK:
		return id3, nil
	case *types.VoteRequest:
		return id2, nil
	case *types.VoteResponse:
		return id1, nil
	case *types.Heartbeat:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *types.LogACK:
		return size3(msg2), nil
	case *types.VoteRequest:
		return size2(msg2), nil
	case *types.VoteResponse:
		return size1(msg2), nil
	case *types.Heartbeat:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *types.LogACK:
		return id3, marshal3(msg2, buf), nil
	case *types.VoteRequest:
		return id2, marshal2(msg2, buf), nil
	case *types.VoteResponse:
		return id1, marshal1(msg2, buf), nil
	case *types.Heartbeat:
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
		msg := &types.LogACK{}
		return msg, unmarshal3(msg, buf), nil
	case id2:
		msg := &types.VoteRequest{}
		return msg, unmarshal2(msg, buf), nil
	case id1:
		msg := &types.VoteResponse{}
		return msg, unmarshal1(msg, buf), nil
	case id0:
		msg := &types.Heartbeat{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *types.LogACK:
		return id3, makePatch3(msg2, msgSrc.(*types.LogACK), buf), nil
	case *types.VoteRequest:
		return id2, makePatch2(msg2, msgSrc.(*types.VoteRequest), buf), nil
	case *types.VoteResponse:
		return id1, makePatch1(msg2, msgSrc.(*types.VoteResponse), buf), nil
	case *types.Heartbeat:
		return id0, makePatch0(msg2, msgSrc.(*types.Heartbeat), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *types.LogACK:
		return applyPatch3(msg2, buf), nil
	case *types.VoteRequest:
		return applyPatch2(msg2, buf), nil
	case *types.VoteResponse:
		return applyPatch1(msg2, buf), nil
	case *types.Heartbeat:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *types.Heartbeat) uint64 {
	var n uint64 = 2
	{
		// Term

		helpers.UInt64Size(m.Term, &n)
	}
	{
		// LeaderCommit

		helpers.UInt64Size(m.LeaderCommit, &n)
	}
	return n
}

func marshal0(m *types.Heartbeat, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Marshal(m.Term, b, &o)
	}
	{
		// LeaderCommit

		helpers.UInt64Marshal(m.LeaderCommit, b, &o)
	}

	return o
}

func unmarshal0(m *types.Heartbeat, b []byte) uint64 {
	var o uint64
	{
		// Term

		helpers.UInt64Unmarshal(&m.Term, b, &o)
	}
	{
		// LeaderCommit

		helpers.UInt64Unmarshal(&m.LeaderCommit, b, &o)
	}

	return o
}

func makePatch0(m, mSrc *types.Heartbeat, b []byte) uint64 {
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
		// LeaderCommit

		if reflect.DeepEqual(m.LeaderCommit, mSrc.LeaderCommit) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(m.LeaderCommit, b, &o)
		}
	}

	return o
}

func applyPatch0(m *types.Heartbeat, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Term, b, &o)
		}
	}
	{
		// LeaderCommit

		if b[0]&0x02 != 0 {
			helpers.UInt64Unmarshal(&m.LeaderCommit, b, &o)
		}
	}

	return o
}

func size1(m *types.VoteResponse) uint64 {
	var n uint64 = 2
	{
		// Term

		helpers.UInt64Size(m.Term, &n)
	}
	return n
}

func marshal1(m *types.VoteResponse, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		helpers.UInt64Marshal(m.Term, b, &o)
	}
	{
		// VoteGranted

		if m.VoteGranted {
			b[0] |= 0x01
		} else {
			b[0] &= 0xFE
		}
	}

	return o
}

func unmarshal1(m *types.VoteResponse, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		helpers.UInt64Unmarshal(&m.Term, b, &o)
	}
	{
		// VoteGranted

		m.VoteGranted = b[0]&0x01 != 0
	}

	return o
}

func makePatch1(m, mSrc *types.VoteResponse, b []byte) uint64 {
	var o uint64 = 2
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
		// VoteGranted

		if m.VoteGranted == mSrc.VoteGranted {
			b[1] &= 0xFE
		} else {
			b[1] |= 0x01
		}
	}

	return o
}

func applyPatch1(m *types.VoteResponse, b []byte) uint64 {
	var o uint64 = 2
	{
		// Term

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Term, b, &o)
		}
	}
	{
		// VoteGranted

		if b[1]&0x01 != 0 {
			m.VoteGranted = !m.VoteGranted
		}
	}

	return o
}

func size2(m *types.VoteRequest) uint64 {
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

func marshal2(m *types.VoteRequest, b []byte) uint64 {
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

func unmarshal2(m *types.VoteRequest, b []byte) uint64 {
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

func makePatch2(m, mSrc *types.VoteRequest, b []byte) uint64 {
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

func applyPatch2(m *types.VoteRequest, b []byte) uint64 {
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

func size3(m *types.LogACK) uint64 {
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

func marshal3(m *types.LogACK, b []byte) uint64 {
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

func unmarshal3(m *types.LogACK, b []byte) uint64 {
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

func makePatch3(m, mSrc *types.LogACK, b []byte) uint64 {
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

func applyPatch3(m *types.LogACK, b []byte) uint64 {
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
