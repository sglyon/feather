package main

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/sglyon/feather"
)

func printRowCol(src *feather.Source, row, col int) string {
	val := src.Columns[col]
	switch v := val.(type) {
	case *feather.BoolColumn:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Int64Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Uint64Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Float64Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Int32Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Uint32Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Float32Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Int16Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Uint16Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Int8Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.Uint8Column:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	case *feather.StringColumn:
		val, _ := v.Value(row)
		return fmt.Sprintf("%v\t", val)
	default:
		return fmt.Sprintf("???\t")
	}
}

func describe(src *feather.Source) {
	fmt.Println(src)
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "\t")
	numColumns := src.NumCols()
	for col := 0; col < numColumns; col++ {
		fmt.Fprintf(w, "%v\t", src.Columns[col].Name())
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "Dtype\t")
	for col := 0; col < numColumns; col++ {
		column := src.Columns[col]
		if column != nil {
			fmt.Fprintf(w, "%v\t", column.TypeString())
		} else {
			fmt.Fprintf(w, "??\t")
		}
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "ArrayType\t")
	for col := 0; col < numColumns; col++ {
		column := src.Columns[col]
		if column != nil {
			fmt.Fprintf(w, "%T\t", column)
		} else {
			fmt.Fprintf(w, "??\t")
		}
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "NumNulls\t")
	for col := 0; col < numColumns; col++ {
		column := src.Columns[col]
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

func printSubset(src *feather.Source) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)

	// Print header
	numColumns := src.NumCols()
	for col := 0; col < numColumns; col++ {
		fmt.Fprintf(w, "%v\t", src.Columns[col].Name())
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
	src, err := feather.Read(fn)
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
