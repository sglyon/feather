package main

import (
	"fmt"

	"github.com/influxdata/arrow/array"
	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
	arrow_feather "github.com/sglyon/feather/with_arrow"
)

func main() {
	src := arrow_feather.Read("/Users/sglyon/Desktop/feather_go/test2.feather")
	numColumns := len(src.Columns)
	vals := src.Columns
	theSeries := make([]series.Series, numColumns)
	for ix := 0; ix < numColumns; ix++ {
		val := vals[ix]
		switch v := val.(type) {
		case *array.Boolean:
			vals := make([]bool, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = v.Value(row)
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Int64:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Uint64:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Float64:
			vals := make([]float64, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = v.Value(row)
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Int32:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Uint32:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Float32:
			vals := make([]float64, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = float64(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Int16:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Uint16:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Int8:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case *array.Uint8:
			vals := make([]int, src.Ctable.NumRows())
			for row := 0; row < len(vals); row++ {
				vals[row] = int(v.Value(row))
			}
			theSeries[ix] = series.New(vals, series.Int, string(cols[ix].Name()))
		case []string:
			theSeries[ix] = series.New(v, series.String, string(cols[ix].Name()))
		default:
			fmt.Printf("I don't know what to do here")
		}
	}
	df := dataframe.New(theSeries...)
	fmt.Println("df:\n", df)
}
