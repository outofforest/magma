package p2p

import (
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

func size0(m *types.Heartbeat) uint64 {
	var n uint64 = 2
	{
		// Term

		{
			vi := m.Term
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
	}
	{
		// LeaderCommit

		{
			vi := m.LeaderCommit
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
	}
	return n
}

func marshal0(m *types.Heartbeat, b []byte) uint64 {
	var o uint64
	{
		// Term

		{
			vi := m.Term
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
	}
	{
		// LeaderCommit

		{
			vi := m.LeaderCommit
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
	}

	return o
}

func unmarshal0(m *types.Heartbeat, b []byte) uint64 {
	var o uint64
	{
		// Term

		{
			vi := types.Term(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Term(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Term(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Term(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Term(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Term(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Term(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Term(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Term(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.Term = vi
		}
	}
	{
		// LeaderCommit

		{
			vi := types.Index(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Index(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Index(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Index(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Index(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Index(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Index(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Index(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Index(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.LeaderCommit = vi
		}
	}

	return o
}

func size1(m *types.VoteResponse) uint64 {
	var n uint64 = 2
	{
		// Term

		{
			vi := m.Term
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
	}
	return n
}

func marshal1(m *types.VoteResponse, b []byte) uint64 {
	var o uint64 = 1
	{
		// Term

		{
			vi := m.Term
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

		{
			vi := types.Term(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Term(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Term(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Term(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Term(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Term(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Term(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Term(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Term(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.Term = vi
		}
	}
	{
		// VoteGranted

		m.VoteGranted = b[0]&0x01 != 0
	}

	return o
}

func size2(m *types.VoteRequest) uint64 {
	var n uint64 = 3
	{
		// Term

		{
			vi := m.Term
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
	}
	{
		// NextLogIndex

		{
			vi := m.NextLogIndex
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
	}
	{
		// LastLogTerm

		{
			vi := m.LastLogTerm
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
	}
	return n
}

func marshal2(m *types.VoteRequest, b []byte) uint64 {
	var o uint64
	{
		// Term

		{
			vi := m.Term
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
	}
	{
		// NextLogIndex

		{
			vi := m.NextLogIndex
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
	}
	{
		// LastLogTerm

		{
			vi := m.LastLogTerm
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
	}

	return o
}

func unmarshal2(m *types.VoteRequest, b []byte) uint64 {
	var o uint64
	{
		// Term

		{
			vi := types.Term(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Term(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Term(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Term(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Term(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Term(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Term(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Term(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Term(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.Term = vi
		}
	}
	{
		// NextLogIndex

		{
			vi := types.Index(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Index(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Index(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Index(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Index(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Index(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Index(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Index(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Index(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.NextLogIndex = vi
		}
	}
	{
		// LastLogTerm

		{
			vi := types.Term(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Term(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Term(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Term(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Term(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Term(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Term(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Term(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Term(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.LastLogTerm = vi
		}
	}

	return o
}

func size3(m *types.LogACK) uint64 {
	var n uint64 = 3
	{
		// Term

		{
			vi := m.Term
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
	}
	{
		// NextLogIndex

		{
			vi := m.NextLogIndex
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
	}
	{
		// SyncLogIndex

		{
			vi := m.SyncLogIndex
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
	}
	return n
}

func marshal3(m *types.LogACK, b []byte) uint64 {
	var o uint64
	{
		// Term

		{
			vi := m.Term
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
	}
	{
		// NextLogIndex

		{
			vi := m.NextLogIndex
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
	}
	{
		// SyncLogIndex

		{
			vi := m.SyncLogIndex
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
	}

	return o
}

func unmarshal3(m *types.LogACK, b []byte) uint64 {
	var o uint64
	{
		// Term

		{
			vi := types.Term(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Term(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Term(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Term(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Term(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Term(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Term(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Term(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Term(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.Term = vi
		}
	}
	{
		// NextLogIndex

		{
			vi := types.Index(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Index(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Index(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Index(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Index(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Index(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Index(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Index(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Index(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.NextLogIndex = vi
		}
	}
	{
		// SyncLogIndex

		{
			vi := types.Index(b[o] & 0x7F)
			if b[o]&0x80 == 0 {
				o++
			} else {
				vi |= types.Index(b[o+1]&0x7F) << 7
				if b[o+1]&0x80 == 0 {
					o += 2
				} else {
					vi |= types.Index(b[o+2]&0x7F) << 14
					if b[o+2]&0x80 == 0 {
						o += 3
					} else {
						vi |= types.Index(b[o+3]&0x7F) << 21
						if b[o+3]&0x80 == 0 {
							o += 4
						} else {
							vi |= types.Index(b[o+4]&0x7F) << 28
							if b[o+4]&0x80 == 0 {
								o += 5
							} else {
								vi |= types.Index(b[o+5]&0x7F) << 35
								if b[o+5]&0x80 == 0 {
									o += 6
								} else {
									vi |= types.Index(b[o+6]&0x7F) << 42
									if b[o+6]&0x80 == 0 {
										o += 7
									} else {
										vi |= types.Index(b[o+7]&0x7F) << 49
										if b[o+7]&0x80 == 0 {
											o += 8
										} else {
											vi |= types.Index(b[o+8]) << 56
											o += 9
										}
									}
								}
							}
						}
					}
				}
			}
			m.SyncLogIndex = vi
		}
	}

	return o
}
