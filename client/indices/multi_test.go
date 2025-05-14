package indices

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	xyz = "XYZ"
	ijk = "IJK"
)

func TestMultiIndexer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)

	index := NewMultiIndex(index1, index2)
	requireT.Equal("index1,index2", index.Name())
	requireT.EqualValues(2, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.False(index.Schema().AllowMissing)

	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	expected := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x58, 0x59, 0x5a, 0x0}
	value, err := indexer.FromArgs(v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestMultiIndexerNotAllArguments(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)

	index := NewMultiIndex(index1, index2)

	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	expected := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5}
	value, err := indexer.FromArgs(v.Value1)
	requireT.NoError(err)
	requireT.Equal(expected, value)
}

func TestMultiIndexerWithMultiSubIndexer3Arguments(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)
	index3 := NewFieldIndex("index3", &v, &v.Value2.Value3)
	index4 := NewMultiIndex(index1, index2)

	index := NewMultiIndex(index3, index4)
	requireT.Equal("index3,index1,index2", index.Name())
	requireT.EqualValues(3, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())

	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	v.Value2.Value3 = ijk
	expected := []byte{0x49, 0x4a, 0x4b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x58, 0x59, 0x5a, 0x0}
	value, err := indexer.FromArgs(v.Value2.Value3, v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestMultiIndexerWithMultiSubIndexer2Arguments(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)
	index3 := NewFieldIndex("index3", &v, &v.Value2.Value3)
	index4 := NewMultiIndex(index1, index2)

	index := NewMultiIndex(index3, index4)
	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	v.Value2.Value3 = ijk
	expected := []byte{0x49, 0x4a, 0x4b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5}
	value, err := indexer.FromArgs(v.Value2.Value3, v.Value1)
	requireT.NoError(err)
	requireT.Equal(expected, value)
}

func TestMultiIndexerWithMultiSubIndexer1Argument(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)
	index3 := NewFieldIndex("index3", &v, &v.Value2.Value3)
	index4 := NewMultiIndex(index1, index2)

	index := NewMultiIndex(index3, index4)

	indexer := index.Schema().Indexer.(*multiIndexer)

	v.Value1 = 5
	v.Value4 = xyz
	v.Value2.Value3 = ijk
	expected := []byte{0x49, 0x4a, 0x4b, 0x0}
	value, err := indexer.FromArgs(v.Value2.Value3)
	requireT.NoError(err)
	requireT.Equal(expected, value)
}

func TestMultiIndexerWithIfSubindex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	var v o

	index1 := NewFieldIndex("index1", &v, &v.Value1)
	index2 := NewFieldIndex("index2", &v, &v.Value4)
	index3 := NewIfIndex("if", index2, ifFunc[o](o{Value1: 1, Value4: abc}, o{Value1: 2, Value4: xyz}))

	index := NewMultiIndex(index1, index3)
	requireT.Equal("index1,index2,if", index.Name())
	requireT.EqualValues(2, index.NumOfArgs())
	requireT.IsType(reflect.TypeOf(o{}), index.Type())
	requireT.True(index.Schema().AllowMissing)

	indexer := index.Schema().Indexer.(*multiIndexer)

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

	v.Value1 = 2
	v.Value4 = xyz
	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x58, 0x59, 0x5a, 0x0}
	value, err = indexer.FromArgs(v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value1 = 1
	v.Value4 = xyz
	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x58, 0x59, 0x5a, 0x0}
	value, err = indexer.FromArgs(v.Value1, v.Value4)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(reflect.ValueOf(&v))
	requireT.NoError(err)
	requireT.False(exists)
	requireT.Nil(value)
}

func TestMultiErrorIfNoSubIndices(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	requireT.Panics(func() {
		NewMultiIndex()
	})
}

func TestMultiErrorOnTypeMismatch(t *testing.T) {
	requireT := require.New(t)
	var v1 o
	var v2 subO1

	index1 := NewFieldIndex("index1", &v1, &v1.Value1)
	index2 := NewFieldIndex("index2", &v2, &v2.Value3)

	requireT.Panics(func() {
		NewMultiIndex(index1, index2)
	})
}
