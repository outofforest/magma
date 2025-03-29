package codec

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/state/events/format"
)

func TestEncoderDecoder(t *testing.T) {
	requireT := require.New(t)

	buf := bytes.NewBuffer(nil)
	m := format.NewMarshaller()

	e := NewEncoder(buf, m)
	requireT.NoError(e.Encode(&format.Term{Term: 1}))
	requireT.NoError(e.Encode(&format.Term{Term: 2}))
	requireT.NoError(e.Encode(&format.Term{Term: 2}))

	d := NewDecoder(buf, m)

	n, v, err := d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 1}, v)
	requireT.EqualValues(11, n)

	n, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 2}, v)
	requireT.EqualValues(22, n)

	n, v, err = d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 2}, v)
	requireT.EqualValues(33, n)

	n, v, err = d.Decode()
	requireT.ErrorIs(err, io.EOF)
	requireT.Nil(v)
	requireT.Zero(n)
}

func TestInvalidChecksum(t *testing.T) {
	requireT := require.New(t)

	buf := bytes.NewBuffer(nil)
	m := format.NewMarshaller()

	e := NewEncoder(buf, m)
	requireT.NoError(e.Encode(&format.Term{Term: 1}))
	requireT.NoError(e.Encode(&format.Term{Term: 2}))

	b := buf.Bytes()
	b[len(b)-1]++

	d := NewDecoder(bytes.NewReader(b), m)

	n, v, err := d.Decode()
	requireT.NoError(err)
	requireT.Equal(&format.Term{Term: 1}, v)
	requireT.EqualValues(11, n)

	n, v, err = d.Decode()
	requireT.Error(err)
	requireT.Nil(v)
	requireT.Zero(n)
}
