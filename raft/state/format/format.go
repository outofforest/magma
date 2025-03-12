package format

import (
	"encoding/binary"

	"github.com/cespare/xxhash"

	"github.com/outofforest/magma/raft/types"
)

const ChecksumSize = 8

type Header struct {
	Term         types.Term
	PreviousTerm types.Term
	NextLogIndex types.Index
	TxOffset     types.Index
	Checksum     uint64
}

func PutChecksum(data []byte) {
	i := len(data) - ChecksumSize
	binary.LittleEndian.PutUint64(data[i:], xxhash.Sum64(data[:i]))
}

func VerifyChecksum(data []byte) bool {
	i := len(data) - ChecksumSize
	return binary.LittleEndian.Uint64(data[i:]) == xxhash.Sum64(data[:i])
}
