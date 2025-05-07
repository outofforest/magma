package indices

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type o struct {
	Value1 uint64
	Value2 subO1
	Value3 subO2
	Value4 string
}

type subO1 struct {
	Value1 uint64
	Value2 subO2
	Value3 string
}

type subO2 struct {
	Value1      string
	Value2      int16
	Value3      uint8
	ValueBool   bool
	ValueString string
	ValueTime   time.Time
	ValueInt8   int8
	ValueInt16  int16
	ValueInt32  int32
	ValueInt64  int64
	ValueUint8  uint8
	ValueUint16 uint16
	ValueUint32 uint32
	ValueUint64 uint64
}

func TestFieldIndexOffset(t *testing.T) {
	requireT := require.New(t)
	v := o{
		Value1: 1,
		Value2: subO1{
			Value1: 2,
			Value2: subO2{
				Value1: "ABC",
				Value2: 2,
				Value3: 3,
			},
			Value3: "DEF",
		},
		Value3: subO2{
			Value1: "GHI",
			Value2: 5,
			Value3: 6,
		},
		Value4: "JKM",
	}

	var v2 o

	_, err := NewFieldIndex("index", &v, &v2)
	requireT.Error(err)

	v3 := &v2
	_, err = NewFieldIndex("index", &v, &v3)
	requireT.Error(err)

	_, err = NewFieldIndex("index", &v, &v)
	requireT.Error(err)

	f := &v.Value1
	_, err = NewFieldIndex("index", &v, &f)
	requireT.Error(err)

	i, err := NewFieldIndex("index", &v, &v.Value1)
	requireT.NoError(err)
	requireT.Equal("index", i.Name())
	requireT.IsType(reflect.TypeOf(o{}), i.Type())
	requireT.Equal("index", i.Schema().Name)
	requireT.Equal(uint64Indexer{
		offset: 0x00,
	}, i.Schema().Indexer)

	_, err = NewFieldIndex("index", &v, &v.Value2)
	requireT.Error(err)

	i, err = NewFieldIndex("index", &v, &v.Value2.Value1)
	requireT.NoError(err)
	requireT.Equal(uint64Indexer{
		offset: 0x08,
	}, i.Schema().Indexer)

	_, err = NewFieldIndex("index", &v, &v.Value2.Value2)
	requireT.Error(err)

	i, err = NewFieldIndex("index", &v, &v.Value2.Value2.Value1)
	requireT.NoError(err)
	requireT.Equal(stringIndexer{
		offset: 0x10,
	}, i.Schema().Indexer)

	i, err = NewFieldIndex("index", &v, &v.Value2.Value2.Value2)
	requireT.NoError(err)
	requireT.Equal(int16Indexer{
		offset: 0x20,
	}, i.Schema().Indexer)

	i, err = NewFieldIndex("index", &v, &v.Value2.Value2.Value3)
	requireT.NoError(err)
	requireT.Equal(uint8Indexer{
		offset: 0x22,
	}, i.Schema().Indexer)

	i, err = NewFieldIndex("index", &v, &v.Value2.Value3)
	requireT.NoError(err)
	requireT.Equal(stringIndexer{
		offset: 0x70,
	}, i.Schema().Indexer)

	_, err = NewFieldIndex("index", &v, &v.Value3)
	requireT.Error(err)

	i, err = NewFieldIndex("index", &v, &v.Value3.Value1)
	requireT.NoError(err)
	requireT.Equal(stringIndexer{
		offset: 0x80,
	}, i.Schema().Indexer)

	i, err = NewFieldIndex("index", &v, &v.Value3.Value2)
	requireT.NoError(err)
	requireT.Equal(int16Indexer{
		offset: 0x90,
	}, i.Schema().Indexer)

	i, err = NewFieldIndex("index", &v, &v.Value3.Value3)
	requireT.NoError(err)
	requireT.Equal(uint8Indexer{
		offset: 0x92,
	}, i.Schema().Indexer)

	i, err = NewFieldIndex("index", &v, &v.Value4)
	requireT.NoError(err)
	requireT.Equal(stringIndexer{
		offset: 0xe0,
	}, i.Schema().Indexer)
}

func TestBoolIndexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueBool)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(boolIndexer)

	v.Value2.Value2.ValueBool = false
	expected := []byte{0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueBool)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueBool = true
	expected = []byte{0x01}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueBool)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestStringIndexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueString)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(stringIndexer)

	v.Value2.Value2.ValueString = ""
	expected := []byte{0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueString)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueString = "ABC"
	expected = []byte{0x41, 0x42, 0x43, 0x00}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueString)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestTimeIndexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueTime)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(timeIndexer)

	v.Value2.Value2.ValueTime = time.Time{}
	expected := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueTime)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueTime = time.Date(2024, 1, 1, 1, 1, 1, 99999, time.UTC)
	expected = []byte{0x80, 0x0, 0x0, 0xe, 0xdd, 0x24, 0x5, 0xcd, 0x0, 0x1, 0x86, 0x9f}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueTime)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueTime = time.Date(-1, 1, 1, 1, 1, 1, 99999, time.UTC)
	expected = []byte{0x7f, 0xff, 0xff, 0xff, 0xfc, 0x3c, 0x55, 0xcd, 0x0, 0x1, 0x86, 0x9f}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueTime)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestInt8Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueInt8)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(int8Indexer)

	v.Value2.Value2.ValueInt8 = 0
	expected := []byte{0x80}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueInt8)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt8 = 127
	expected = []byte{0xff}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt8)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt8 = -128
	expected = []byte{0x00}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt8)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestInt16Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueInt16)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(int16Indexer)

	v.Value2.Value2.ValueInt16 = 0
	expected := []byte{0x80, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueInt16)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt16 = 30000
	expected = []byte{0xf5, 0x30}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt16)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt16 = -30000
	expected = []byte{0x0a, 0xd0}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt16)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestInt32Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueInt32)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(int32Indexer)

	v.Value2.Value2.ValueInt32 = 0
	expected := []byte{0x80, 0x00, 0x00, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueInt32)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt32 = 300000000
	expected = []byte{0x91, 0xe1, 0xa3, 0x0}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt32)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt32 = -300000000
	expected = []byte{0x6e, 0x1e, 0x5d, 0x0}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt32)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestInt64Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueInt64)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(int64Indexer)

	v.Value2.Value2.ValueInt64 = 0
	expected := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueInt64)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt64 = 3000000000000000000
	expected = []byte{0xa9, 0xa2, 0x24, 0x1a, 0xf6, 0x2c, 0x0, 0x0}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt64)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueInt64 = -3000000000000000000
	expected = []byte{0x56, 0x5d, 0xdb, 0xe5, 0x9, 0xd4, 0x0, 0x0}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueInt64)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestUInt8Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueUint8)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(uint8Indexer)

	v.Value2.Value2.ValueUint8 = 0
	expected := []byte{0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueUint8)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueUint8 = math.MaxUint8
	expected = []byte{0xff}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueUint8)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestUInt16Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueUint16)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(uint16Indexer)

	v.Value2.Value2.ValueUint16 = 0
	expected := []byte{0x00, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueUint16)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueUint16 = math.MaxUint16
	expected = []byte{0xff, 0xff}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueUint16)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestUInt32Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueUint32)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(uint32Indexer)

	v.Value2.Value2.ValueUint32 = 0
	expected := []byte{0x00, 0x00, 0x00, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueUint32)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueUint32 = math.MaxUint32
	expected = []byte{0xff, 0xff, 0xff, 0xff}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueUint32)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}

func TestUInt64Indexer(t *testing.T) {
	requireT := require.New(t)
	var v o

	index, err := NewFieldIndex("index", &v, &v.Value2.Value2.ValueUint64)
	requireT.NoError(err)
	indexer := index.Schema().Indexer.(uint64Indexer)

	v.Value2.Value2.ValueUint64 = 0
	expected := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	value, err := indexer.FromArgs(v.Value2.Value2.ValueUint64)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err := indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)

	v.Value2.Value2.ValueUint64 = math.MaxUint64
	expected = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	value, err = indexer.FromArgs(v.Value2.Value2.ValueUint64)
	requireT.NoError(err)
	requireT.Equal(expected, value)
	exists, value, err = indexer.FromObject(v)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(expected, value)
}
