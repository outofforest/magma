package indices

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func ifFunc[T comparable](values ...T) func(o *T) bool {
	vs := map[T]bool{}
	for _, v := range values {
		vs[v] = true
	}

	return func(o *T) bool {
		return vs[*o]
	}
}

func TestIfIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	subIndex := NewFieldIndex("index", &v, &v.Value1)

	index := NewIfIndex("if", subIndex, ifFunc[o](o{Value1: 1}, o{Value1: 2}))
	requireT.Equal("index,if", index.Name())
	requireT.EqualValues(1, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.True(index.Schema().AllowMissing)

	indexer := index.Schema().Indexer.(ifIndexer[o])

	v.Value1 = 1
	expected := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}
	value, err := indexer.FromArgs(v.Value1)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value1 = 2
	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2}
	value, err = indexer.FromArgs(v.Value1)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value1 = 3
	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3}
	value, err = indexer.FromArgs(v.Value1)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.False(exists)
	requireT.Nil(value)
}

func TestIfIndexerMulti(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)

	subIndex := NewMultiIndex(index1, index2)

	index := NewIfIndex("if", subIndex, ifFunc[o](o{Value1: 1, Value4: abc}, o{Value1: 1, Value4: def}))
	requireT.Equal("index1,index2,if", index.Name())
	requireT.EqualValues(2, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(ifIndexer[o])

	v.Value1 = 1
	v.Value4 = abc
	expected := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x41, 0x42, 0x43, 0x0}
	value, err := indexer.FromArgs(v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value1 = 1
	v.Value4 = def
	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x44, 0x45, 0x46, 0x0}
	value, err = indexer.FromArgs(v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value1 = 2
	v.Value4 = abc
	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x41, 0x42, 0x43, 0x0}
	value, err = indexer.FromArgs(v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.False(exists)
	requireT.Nil(value)
}

func TestIfIndexerErrorOnTypeMismatch(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	subIndex := NewFieldIndex("index", &v, &v.Value1)

	requireT.Panics(func() {
		NewIfIndex("if", subIndex, ifFunc[subO1](subO1{Value1: 1}, subO1{Value1: 2}))
	})
}
