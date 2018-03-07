package main

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/influxdata/arrow/array"
	arrow_feather "github.com/sglyon/feather/with_arrow"
)

func printRowCol(src *arrow_feather.Source, row, col int) string {
	val := src.Columns[src.ColNames[col]]
	switch v := val.(type) {
	case *array.Boolean:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int64:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint64:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Float64:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int32:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint32:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Float32:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int16:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint16:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int8:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint8:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Timestamp:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int64Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint64Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Float64Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int32Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint32Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Float32Dict:
		return fmt.Sprintf("%v\t", v.Refs[row])
	case *array.Int16Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint16Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Int8Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.Uint8Dict:
		return fmt.Sprintf("%v\t", v.Value(row))
	case *array.TimestampDict:
		return fmt.Sprintf("%v\t", v.Value(row))
	default:
		return fmt.Sprintf("???\t")
	}
}

func describe(src *arrow_feather.Source) {
	fmt.Println(src)
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "\t")
	numColumns := src.NumCols()
	for col := 0; col < numColumns; col++ {
		fmt.Fprintf(w, "%v\t", src.ColNames[col])
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "Dtype\t")
	for col := 0; col < numColumns; col++ {
		column := src.Columns[src.ColNames[col]]
		if column != nil {
			fmt.Fprintf(w, "%v\t", column.DataType().Name())
		} else {
			fmt.Fprintf(w, "??\t")
		}
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "ArrayType\t")
	for col := 0; col < numColumns; col++ {
		column := src.Columns[src.ColNames[col]]
		if column != nil {
			fmt.Fprintf(w, "%T\t", column)
		} else {
			fmt.Fprintf(w, "??\t")
		}
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "NumNulls\t")
	for col := 0; col < numColumns; col++ {
		column := src.Columns[src.ColNames[col]]
		if column != nil {
			fmt.Fprintf(w, "%v\t", column.NullN())
		} else {
			fmt.Fprintf(w, "??\t")
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w)
	w.Flush()

	printSubset(src)
}

func printSubset(src *arrow_feather.Source) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)

	// Print header
	numColumns := src.NumCols()
	for col := 0; col < numColumns; col++ {
		fmt.Fprintf(w, "%v\t", src.ColNames[col])
	}
	fmt.Fprintln(w)

	// If there are 20 rows or less, print all. Otherwise print the top 10 and
	// bottom 10
	numRows := src.NumRows()
	if numRows <= 20 {
		for row := 0; row < numRows; row++ {
			for col := 0; col < numColumns; col++ {
				fmt.Fprintf(w, printRowCol(src, row, col))
			}
			fmt.Fprintln(w)
		}
	} else {
		for row := 0; row < 10; row++ {
			for col := 0; col < numColumns; col++ {
				fmt.Fprintf(w, printRowCol(src, row, col))
			}
			fmt.Fprintln(w)
		}

		for col := 0; col < numColumns; col++ {
			fmt.Fprintf(w, "⋮\t")
		}
		fmt.Fprintln(w)

		for row := numRows - 10; row < numRows; row++ {
			for col := 0; col < numColumns; col++ {
				fmt.Fprintf(w, printRowCol(src, row, col))
			}
			fmt.Fprintln(w)
		}

	}

	w.Flush()
}

func describeFile(fn string) {
	fmt.Printf("\n")
	src, err := arrow_feather.Read(fn)
	if err != nil {
		fmt.Printf("Unable to handle file %s  -- error was '%v'\n", fn, err)
		return
	}
	describe(src)
}

func main() {
	patterns := os.Args[1:]
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			fmt.Printf("Couldn't use glob pattern %v\n", pattern)
			continue
		}
		for _, file := range matches {
			describeFile(file)
		}
	}
}
