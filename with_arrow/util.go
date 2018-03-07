package arrow_feather

import (
	"github.com/influxdata/arrow"
	"github.com/sglyon/feather/fbs"
)

var fbsToArrowType = map[int8]arrow.Type{
	fbs.TypEBOOL:      arrow.BOOL,
	fbs.TypEINT8:      arrow.INT8,
	fbs.TypEINT16:     arrow.INT16,
	fbs.TypEINT32:     arrow.INT32,
	fbs.TypEINT64:     arrow.INT64,
	fbs.TypEUINT8:     arrow.UINT8,
	fbs.TypEUINT16:    arrow.UINT16,
	fbs.TypEUINT32:    arrow.UINT32,
	fbs.TypEUINT64:    arrow.UINT64,
	fbs.TypEFLOAT:     arrow.FLOAT32,
	fbs.TypEDOUBLE:    arrow.FLOAT64,
	fbs.TypEUTF8:      arrow.STRING,
	fbs.TypEBINARY:    arrow.BINARY,
	fbs.TypECATEGORY:  arrow.DICTIONARY,
	fbs.TypETIMESTAMP: arrow.TIMESTAMP,
	// TODO: does feather use DATE64 and TIME64 or DATE32 and TIME32.
	// From here I think it is 64 https://github.com/apache/arrow/blob/5994094e2a963ba22abd657121935e2ddbfa8660/cpp/src/arrow/ipc/feather.cc#L475-L481
	// fbs.TypEDATE:      arrow.DATE64,
	// fbs.TypETIME:      arrow.TIME64,
}

var fbsToArrowDataType = map[int8]arrow.DataType{
	fbs.TypEBOOL:   arrow.FixedWidthTypes.Boolean,
	fbs.TypEINT8:   arrow.PrimitiveTypes.Int8,
	fbs.TypEINT16:  arrow.PrimitiveTypes.Int16,
	fbs.TypEINT32:  arrow.PrimitiveTypes.Int32,
	fbs.TypEINT64:  arrow.PrimitiveTypes.Int64,
	fbs.TypEUINT8:  arrow.PrimitiveTypes.Uint8,
	fbs.TypEUINT16: arrow.PrimitiveTypes.Uint16,
	fbs.TypEUINT32: arrow.PrimitiveTypes.Uint32,
	fbs.TypEUINT64: arrow.PrimitiveTypes.Uint64,
	fbs.TypEFLOAT:  arrow.PrimitiveTypes.Float32,
	fbs.TypEDOUBLE: arrow.PrimitiveTypes.Float64,
	// fbs.TypEUTF8:      arrow.STRING,
	// fbs.TypEBINARY:    arrow.BINARY,
	// fbs.TypECATEGORY:  arrow.DICTIONARY,
	// fbs.TypETIMESTAMP: arrow.TIMESTAMP,
	// TODO: does feather use DATE64 and TIME64 or DATE32 and TIME32.
	// From here I think it is 64 https://github.com/apache/arrow/blob/5994094e2a963ba22abd657121935e2ddbfa8660/cpp/src/arrow/ipc/feather.cc#L475-L481
	// fbs.TypEDATE:      arrow.DATE64,
	// fbs.TypETIME:      arrow.TIME64,
}
