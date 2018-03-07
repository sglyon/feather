package main

import (
	"fmt"

	"github.com/influxdata/arrow/array"
	arrow_feather "github.com/sglyon/feather/with_arrow"
)

func describe(src *arrow_feather.Source) {
	numColumns := len(src.ColNames)
	for ix := 0; ix < numColumns; ix++ {
		fmt.Printf("%10v:\t", string(src.ColNames[ix]))
		val := src.Columns[src.ColNames[ix]]
		switch v := val.(type) {
		case *array.Boolean:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int64:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint64:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Float64:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int32:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint32:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Float32:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int16:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint16:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int8:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint8:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Timestamp:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int64Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint64Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Float64Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int32Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint32Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Float32Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Refs[row])
			}
		case *array.Int16Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint16Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Int8Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.Uint8Dict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		case *array.TimestampDict:
			for row := 0; row < int(src.Ctable.NumRows()); row++ {
				fmt.Printf("%v\t", v.Value(row))
			}
		// case []string:
		// 	for row := 0; row < int(src.Ctable.NumRows()); row++ {
		// 		fmt.Printf("%v\t", v[row])
		// 	}
		default:
			fmt.Printf("I don't know what to do here")
		}
		fmt.Println()
	}
}

func describeFile(fn string) {
	fmt.Println("\n\n\nWorking on file", fn)
	fmt.Println("")
	src := arrow_feather.Read(fn)
	describe(src)
}

func main() {
	describeFile("/Users/sglyon/gocode/src/github.com/sglyon/feather/test_data/test2.feather")
	describeFile("/Users/sglyon/gocode/src/github.com/sglyon/feather/test_data/cats.feather")
	describeFile("/Users/sglyon/gocode/src/github.com/sglyon/feather/test_data/test2missing.feather")
}
