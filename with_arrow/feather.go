package arrow_feather

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influxdata/arrow/array"
	"github.com/influxdata/arrow/memory"
	"github.com/sglyon/feather/fbs"
	"golang.org/x/exp/mmap"
)

const alignment = 8

func paddedlength(x int64) int64 {
	return ((x + alignment - 1) / alignment) * alignment
}

// getoutputlength gets the length of the output part of a variable encoded
// size buffer -- depending on version of Feather file. Version >= 2 has 8 byte
// alignment, whereas earlier versions don't force alignment.
func getoutputlength(version int32, x int64) int64 {
	if version < 2 {
		return x
	}
	return paddedlength(x)
}

// Source holds the source for the feather file
type Source struct {
	Data       *mmap.ReaderAt
	Ctable     *fbs.CTable
	fbsColumns []*fbs.Column
	ColNames   []string
	Columns    map[string]array.Interface
}

// Read reads a feather file, parses metadata, and returns a Source
func Read(fn string) *Source {
	file, err := mmap.Open(fn)
	if err != nil {
		panic(err)
	}
	length := int64(file.Len())

	magic := make([]byte, 4)

	file.ReadAt(magic, 0)
	if string(magic) != "FEA1" {
		panic("didn't find magic bytes in header")
	}

	file.ReadAt(magic, length)
	if string(magic) != "FEA1" {
		panic("didn't find magic bytes in footer")
	}

	// Read metadata
	metadataSizeBuf := make([]byte, 4)
	file.ReadAt(metadataSizeBuf, length-8)
	var metadataSize int32
	binary.Read(bytes.NewBuffer(metadataSizeBuf), binary.LittleEndian, &metadataSize)

	// Read CTable
	ctableBuf := make([]byte, metadataSize)
	file.ReadAt(ctableBuf, length-8-int64(metadataSize))
	ctable := fbs.GetRootAsCTable(ctableBuf, 0)

	numColumns := ctable.ColumnsLength()
	cols := make([]*fbs.Column, numColumns)
	colnames := make([]string, numColumns)
	vals := make(map[string]array.Interface, numColumns)
	src := Source{file, ctable, cols, colnames, vals}

	// Parse all the columns in parallel -- use a sync.Mutex to protect
	// write to the vals map
	var mutex sync.Mutex
	wg := &sync.WaitGroup{}

	for ix := 0; ix < numColumns; ix++ {
		wg.Add(1)
		go func(ii int) {
			defer wg.Done()
			col := new(fbs.Column)
			ctable.Columns(col, ii)
			cols[ii] = col
			colnames[ii] = string(col.Name())
			parsed := parseCol(&src, cols[ii])
			mutex.Lock()
			vals[colnames[ii]] = parsed
			mutex.Unlock()
		}(ix)
	}
	wg.Wait()

	// TODO: NullCount
	return &src
}

func (src *Source) getoutputlength(x int64) int64 {
	return getoutputlength(src.Ctable.Version(), x)
}

func ensureInt32Data(in array.Interface) *array.Int32 {
	switch v := in.(type) {
	case *array.Int32:
		return v
	case *array.Int8:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	case *array.Int16:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	case *array.Int64:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	case *array.Uint8:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	case *array.Uint16:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	case *array.Uint32:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	case *array.Uint64:
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ab := array.NewInt32Builder(mem)
		for ix := 0; ix < v.Len(); ix++ {
			ab.Append(int32(v.Value(ix)))
		}
		return ab.NewInt32Array()
	default:
		panic("Can only convert integer like columns to Int32")
	}
}

func parseCol(src *Source, col *fbs.Column) array.Interface {

	switch col.MetadataType() {
	case fbs.TypeMetadataNONE:
		return arrayForPrimitive(src, col.Values(nil))
	case fbs.TypeMetadataCategoryMetadata:
		vals := arrayForPrimitive(src, col.Values(nil))
		meta := metadataForCol(src, col)
		return array.MakeDictFromData(ensureInt32Data(vals).Data(), meta.Data())

	case fbs.TypeMetadataTimestampMetadata:
		fmt.Println("Have TypeMetadataTimestampMetadata")
		return nil
	case fbs.TypeMetadataDateMetadata:
		fmt.Println("Have TypeMetadataDateMetadata")
		return nil
	case fbs.TypeMetadataTimeMetadata:
		fmt.Println("Have TypeMetadataTimeMetadata")
		return nil
	}
	return nil
}

func arrayForPrimitive(src *Source, vals *fbs.PrimitiveArray) array.Interface { // TODO: should return array.Interface
	length := vals.Length()

	// extract bytes for null bitmap
	var numBitmaskBytes int64
	if vals.NullCount() > 0 {
		numBitmaskBytes += src.getoutputlength(length / 8)
	}
	nullBuf := make([]byte, numBitmaskBytes)
	src.Data.ReadAt(nullBuf, vals.Offset())
	arrowNullBuf := memory.NewBufferBytes(nullBuf)

	// extract butes for value
	valsBuf := make([]byte, vals.TotalBytes()-numBitmaskBytes)
	src.Data.ReadAt(valsBuf, vals.Offset()+numBitmaskBytes)

	arrowBuf := memory.NewBufferBytes(valsBuf)
	arrowData := array.NewData(fbsToArrowDataType[vals.TypE()], int(length), []*memory.Buffer{arrowNullBuf, arrowBuf}, 0)

	switch vals.TypE() {
	case fbs.TypEBOOL, fbs.TypEINT32, fbs.TypEUINT64, fbs.TypEINT64, fbs.TypEDOUBLE, fbs.TypEBINARY, fbs.TypEINT8, fbs.TypEINT16, fbs.TypEUINT8, fbs.TypEUINT16, fbs.TypEUINT32, fbs.TypEFLOAT:
		return array.MakeFromData(arrowData)

	case fbs.TypEUTF8:
		// First parse offsets
		offsets := make([]uint32, length+1)
		binary.Read(bytes.NewBuffer(valsBuf), binary.LittleEndian, &offsets)

		// chop offsets off of valsBuf
		outlen := getoutputlength(src.Ctable.Version(), 4*(length+1))
		valsBuf = valsBuf[outlen:]
		// Then parse values
		out := make([]string, length)

		for ix := 0; ix < int(length); ix++ {
			out[ix] = string(valsBuf[offsets[ix]:offsets[ix+1]])
		}

		return nil
		// return out

	case fbs.TypECATEGORY:
		fmt.Println("Have TypECATEGORY")
		return nil
	case fbs.TypETIMESTAMP:
		fmt.Println("Have TypETIMESTAMP")
		return nil
	case fbs.TypEDATE:
		fmt.Println("Have TypEDATE")
		return nil
	case fbs.TypETIME:
		fmt.Println("Have TypETIME")
		return nil
	}
	return nil
}

func parseCategoryMetadata(src *Source, col *fbs.Column) array.Interface {
	if col.MetadataType() != fbs.TypeMetadataCategoryMetadata {
		return nil
	}
	var _catMeta flatbuffers.Table
	inited := col.Metadata(&_catMeta)
	if !inited {
		panic("couldn't load metadata")
	}

	var catmeta fbs.CategoryMetadata
	catmeta.Init(_catMeta.Bytes, _catMeta.Pos)
	lvls := catmeta.Levels(nil)

	return arrayForPrimitive(src, lvls)
}

func metadataForCol(src *Source, col *fbs.Column) array.Interface {
	// Now work on metadta
	switch col.MetadataType() {
	case fbs.TypeMetadataNONE:
		return nil // correct behaviors
	case fbs.TypeMetadataCategoryMetadata:
		return parseCategoryMetadata(src, col)
	case fbs.TypeMetadataTimestampMetadata:
		fmt.Println("Have TypeMetadataTimestampMetadata")
	case fbs.TypeMetadataDateMetadata:
		fmt.Println("Have TypeMetadataDateMetadata")
	case fbs.TypeMetadataTimeMetadata:
		fmt.Println("Have TypeMetadataTimeMetadata")
	}
	return nil
}

// func (src *Source) At(i, j int) interface{} {
// 	return src.Columns[src.ColNames[j]].Value(i)
// }
