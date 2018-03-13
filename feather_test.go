package feather

import (
	"math"
	"testing"
)

// Julia code to generate test dataframes is in the file gen_test_data.jl

func TestTest2Feather(t *testing.T) {
	fn := "test_data/test2.feather"
	src, err := Read(fn)
	if err != nil {
		t.Fail()
	}
	if src.NumCols() != 12 {
		t.Error("Expected 12 columns in test2.feather")
	}

	if src.NumRows() != 10 {
		t.Error("Expected 10 columns in test2.feather")
	}

	want0 := []bool{true, true, false, true, true, true, false, false, false, true}
	col0 := src.Columns[0].(*BoolColumn)
	full0, errfull0 := col0.ToFullColumn()
	fullvals0 := full0.Values()
	vals0, valids0 := col0.Values()

	want1 := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	col1 := src.Columns[1].(*Int8Column)
	full1, errfull1 := col1.ToFullColumn()
	fullvals1 := full1.Values()
	vals1, valids1 := col1.Values()

	want2 := []int32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	col2 := src.Columns[2].(*Int32Column)
	full2, errfull2 := col2.ToFullColumn()
	fullvals2 := full2.Values()
	vals2, valids2 := col2.Values()

	want3 := []int16{3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	col3 := src.Columns[3].(*Int16Column)
	full3, errfull3 := col3.ToFullColumn()
	fullvals3 := full3.Values()
	vals3, valids3 := col3.Values()

	want4 := []int64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	col4 := src.Columns[4].(*Int64Column)
	full4, errfull4 := col4.ToFullColumn()
	fullvals4 := full4.Values()
	vals4, valids4 := col4.Values()

	want5 := []uint8{0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e}
	col5 := src.Columns[5].(*Uint8Column)
	full5, errfull5 := col5.ToFullColumn()
	fullvals5 := full5.Values()
	vals5, valids5 := col5.Values()

	want6 := []uint16{0x0006, 0x0007, 0x0008, 0x0009, 0x000a, 0x000b, 0x000c, 0x000d, 0x000e, 0x000f}
	col6 := src.Columns[6].(*Uint16Column)
	full6, errfull6 := col6.ToFullColumn()
	fullvals6 := full6.Values()
	vals6, valids6 := col6.Values()

	want7 := []uint32{0x00000007, 0x00000008, 0x00000009, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000d, 0x0000000e, 0x0000000f, 0x00000010}
	col7 := src.Columns[7].(*Uint32Column)
	full7, errfull7 := col7.ToFullColumn()
	fullvals7 := full7.Values()
	vals7, valids7 := col7.Values()

	want8 := []uint64{0x0000000000000008, 0x0000000000000009, 0x000000000000000a, 0x000000000000000b, 0x000000000000000c, 0x000000000000000d, 0x000000000000000e, 0x000000000000000f, 0x0000000000000010, 0x0000000000000011}
	col8 := src.Columns[8].(*Uint64Column)
	full8, errfull8 := col8.ToFullColumn()
	fullvals8 := full8.Values()
	vals8, valids8 := col8.Values()

	want9 := []float32{9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0}
	col9 := src.Columns[9].(*Float32Column)
	full9, errfull9 := col9.ToFullColumn()
	fullvals9 := full9.Values()
	vals9, valids9 := col9.Values()

	want10 := []float64{10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0}
	col10 := src.Columns[10].(*Float64Column)
	full10, errfull10 := col10.ToFullColumn()
	fullvals10 := full10.Values()
	vals10, valids10 := col10.Values()

	want11 := []string{"AB", "CD", "EF", "GH", "IJ", "KL", "MN", "OP", "QR", "ST"}
	col11 := src.Columns[11].(*StringColumn)
	full11, errfull11 := col11.ToFullColumn()
	fullvals11 := full11.Values()
	vals11, valids11 := col11.Values()

	if valids0 != nil {
		t.Error("valids0 not nil")
	}
	if valids1 != nil {
		t.Error("valids1 not nil")
	}
	if valids2 != nil {
		t.Error("valids2 not nil")
	}
	if valids3 != nil {
		t.Error("valids3 not nil")
	}
	if valids4 != nil {
		t.Error("valids4 not nil")
	}
	if valids5 != nil {
		t.Error("valids5 not nil")
	}
	if valids6 != nil {
		t.Error("valids6 not nil")
	}
	if valids7 != nil {
		t.Error("valids7 not nil")
	}
	if valids8 != nil {
		t.Error("valids8 not nil")
	}
	if valids9 != nil {
		t.Error("valids9 not nil")
	}
	if valids10 != nil {
		t.Error("valids10 not nil")
	}
	if valids11 != nil {
		t.Error("valids11 not nil")
	}

	if errfull0 != nil {
		t.Error("error0 not nil")
	}
	if errfull1 != nil {
		t.Error("error1 not nil")
	}
	if errfull2 != nil {
		t.Error("error2 not nil")
	}
	if errfull3 != nil {
		t.Error("error3 not nil")
	}
	if errfull4 != nil {
		t.Error("error4 not nil")
	}
	if errfull5 != nil {
		t.Error("error5 not nil")
	}
	if errfull6 != nil {
		t.Error("error6 not nil")
	}
	if errfull7 != nil {
		t.Error("error7 not nil")
	}
	if errfull8 != nil {
		t.Error("error8 not nil")
	}
	if errfull9 != nil {
		t.Error("error9 not nil")
	}
	if errfull10 != nil {
		t.Error("error10 not nil")
	}
	if errfull11 != nil {
		t.Error("error11 not nil")
	}

	for ix := 0; ix < 10; ix++ {
		val0, isvalid0 := col0.Value(ix)
		val1, isvalid1 := col1.Value(ix)
		val2, isvalid2 := col2.Value(ix)
		val3, isvalid3 := col3.Value(ix)
		val4, isvalid4 := col4.Value(ix)
		val5, isvalid5 := col5.Value(ix)
		val6, isvalid6 := col6.Value(ix)
		val7, isvalid7 := col7.Value(ix)
		val8, isvalid8 := col8.Value(ix)
		val9, isvalid9 := col9.Value(ix)
		val10, isvalid10 := col10.Value(ix)
		val11, isvalid11 := col11.Value(ix)

		if want0[ix] != val0 || vals0[ix] != want0[ix] || fullvals0[ix] != want0[ix] || !isvalid0 {
			t.Errorf("Row %v, Col 0 error. want0[%v] = %v and val0 = %v and vals0[ix] = %v and isvalid0 = %v", ix, ix, want0[ix], val0, vals0[ix], isvalid0)
		}
		if want1[ix] != val1 || vals1[ix] != want1[ix] || fullvals1[ix] != want1[ix] || !isvalid1 {
			t.Errorf("Row %v, Col 1 error. want1[%v] = %v and val1 = %v and vals1[ix] = %v and isvalid1 = %v", ix, ix, want1[ix], val1, vals0[ix], isvalid0)
		}
		if want2[ix] != val2 || vals2[ix] != want2[ix] || fullvals2[ix] != want2[ix] || !isvalid2 {
			t.Errorf("Row %v, Col 2 error. want2[%v] = %v and val2 = %v and vals2[ix] = %v and isvalid2 = %v", ix, ix, want2[ix], val2, vals0[ix], isvalid0)
		}
		if want3[ix] != val3 || vals3[ix] != want3[ix] || fullvals3[ix] != want3[ix] || !isvalid3 {
			t.Errorf("Row %v, Col 3 error. want3[%v] = %v and val3 = %v and vals3[ix] = %v and isvalid3 = %v", ix, ix, want3[ix], val3, vals0[ix], isvalid0)
		}
		if want4[ix] != val4 || vals4[ix] != want4[ix] || fullvals4[ix] != want4[ix] || !isvalid4 {
			t.Errorf("Row %v, Col 4 error. want4[%v] = %v and val4 = %v and vals4[ix] = %v and isvalid4 = %v", ix, ix, want4[ix], val4, vals0[ix], isvalid0)
		}
		if want5[ix] != val5 || vals5[ix] != want5[ix] || fullvals5[ix] != want5[ix] || !isvalid5 {
			t.Errorf("Row %v, Col 5 error. want5[%v] = %v and val5 = %v and vals5[ix] = %v and isvalid5 = %v", ix, ix, want5[ix], val5, vals0[ix], isvalid0)
		}
		if want6[ix] != val6 || vals6[ix] != want6[ix] || fullvals6[ix] != want6[ix] || !isvalid6 {
			t.Errorf("Row %v, Col 6 error. want6[%v] = %v and val6 = %v and vals6[ix] = %v and isvalid6 = %v", ix, ix, want6[ix], val6, vals0[ix], isvalid0)
		}
		if want7[ix] != val7 || vals7[ix] != want7[ix] || fullvals7[ix] != want7[ix] || !isvalid7 {
			t.Errorf("Row %v, Col 7 error. want7[%v] = %v and val7 = %v and vals7[ix] = %v and isvalid7 = %v", ix, ix, want7[ix], val7, vals0[ix], isvalid0)
		}
		if want8[ix] != val8 || vals8[ix] != want8[ix] || fullvals8[ix] != want8[ix] || !isvalid8 {
			t.Errorf("Row %v, Col 8 error. want8[%v] = %v and val8 = %v and vals8[ix] = %v and isvalid8 = %v", ix, ix, want8[ix], val8, vals0[ix], isvalid0)
		}
		if want9[ix] != val9 || vals9[ix] != want9[ix] || fullvals9[ix] != want9[ix] || !isvalid9 {
			t.Errorf("Row %v, Col 9 error. want9[%v] = %v and val9 = %v and vals9[ix] = %v and isvalid9 = %v", ix, ix, want9[ix], val9, vals0[ix], isvalid0)
		}
		if want10[ix] != val10 || vals10[ix] != want10[ix] || fullvals10[ix] != want10[ix] || !isvalid10 {
			t.Errorf("Row %v, Col 10 error. want10[%v] = %v and val10 = %v and vals10[ix] = %v and isvalid10 = %v", ix, ix, want10[ix], val10, vals0[ix], isvalid0)
		}
		if want11[ix] != val11 || vals11[ix] != want11[ix] || fullvals11[ix] != want11[ix] || !isvalid11 {
			t.Errorf("Row %v, Col 11 error. want11[%v] = %v and val11 = %v and vals11[ix] = %v and isvalid11 = %v", ix, ix, want11[ix], val11, vals0[ix], isvalid0)
		}
	}
}

func TestTest2MissingFeather(t *testing.T) {
	fn := "test_data/test2missing.feather"
	src, err := Read(fn)
	if err != nil {
		t.Fail()
	}

	want0 := []bool{true, true, false, true, true, true, false, false, false, true}
	col0 := src.Columns[0].(*BoolColumn)
	full0, errfull0 := col0.ToFullColumn()
	vals0, valids0 := col0.Values()

	want1 := []int8{1, 2, 0, 4, 5, 6, 7, 8, 0, 10}
	col1 := src.Columns[1].(*Int8Column)
	full1, errfull1 := col1.ToFullColumn()
	vals1, valids1 := col1.Values()

	want2 := []int32{2, 3, 0, 5, 6, 7, 8, 9, 0, 11}
	col2 := src.Columns[2].(*Int32Column)
	full2, errfull2 := col2.ToFullColumn()
	vals2, valids2 := col2.Values()

	want3 := []int16{3, 4, 0, 6, 7, 8, 9, 10, 0, 12}
	col3 := src.Columns[3].(*Int16Column)
	full3, errfull3 := col3.ToFullColumn()
	vals3, valids3 := col3.Values()

	want4 := []int64{4, 5, 0, 7, 8, 9, 10, 11, 0, 13}
	col4 := src.Columns[4].(*Int64Column)
	full4, errfull4 := col4.ToFullColumn()
	vals4, valids4 := col4.Values()

	want5 := []uint8{0x05, 0x06, 0x00, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x00, 0x0e}
	col5 := src.Columns[5].(*Uint8Column)
	full5, errfull5 := col5.ToFullColumn()
	vals5, valids5 := col5.Values()

	want6 := []uint16{0x0006, 0x0007, 0x0000, 0x0009, 0x000a, 0x000b, 0x000c, 0x000d, 0x0000, 0x000f}
	col6 := src.Columns[6].(*Uint16Column)
	full6, errfull6 := col6.ToFullColumn()
	vals6, valids6 := col6.Values()

	want7 := []uint32{0x00000007, 0x00000008, 0x00000000, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000d, 0x0000000e, 0x00000000, 0x00000010}
	col7 := src.Columns[7].(*Uint32Column)
	full7, errfull7 := col7.ToFullColumn()
	vals7, valids7 := col7.Values()

	want8 := []uint64{0x0000000000000008, 0x0000000000000009, 0x0000000000000000, 0x000000000000000b, 0x000000000000000c, 0x000000000000000d, 0x000000000000000e, 0x000000000000000f, 0x0000000000000000, 0x0000000000000011}
	col8 := src.Columns[8].(*Uint64Column)
	full8, errfull8 := col8.ToFullColumn()
	vals8, valids8 := col8.Values()

	want9 := []float32{9.0, 10.0, 0.0, 12.0, 13.0, 14.0, 15.0, 16.0, 0.0, 18.0}
	col9 := src.Columns[9].(*Float32Column)
	full9, errfull9 := col9.ToFullColumn()
	vals9, valids9 := col9.Values()

	want10 := []float64{10.0, 11.0, 0.0, 13.0, 14.0, 15.0, 16.0, 17.0, 0.0, 19.0}
	col10 := src.Columns[10].(*Float64Column)
	full10, errfull10 := col10.ToFullColumn()
	vals10, valids10 := col10.Values()

	want11 := []string{"AB", "CD", "", "GH", "IJ", "KL", "MN", "OP", "", "ST"}
	col11 := src.Columns[11].(*StringColumn)
	full11, errfull11 := col11.ToFullColumn()
	vals11, valids11 := col11.Values()

	if valids0 == nil {
		t.Error("valids0 shoud not be nil")
	}
	if valids1 == nil {
		t.Error("valids1 shoud not be nil")
	}
	if valids2 == nil {
		t.Error("valids2 shoud not be nil")
	}
	if valids3 == nil {
		t.Error("valids3 shoud not be nil")
	}
	if valids4 == nil {
		t.Error("valids4 shoud not be nil")
	}
	if valids5 == nil {
		t.Error("valids5 shoud not be nil")
	}
	if valids6 == nil {
		t.Error("valids6 shoud not be nil")
	}
	if valids7 == nil {
		t.Error("valids7 shoud not be nil")
	}
	if valids8 == nil {
		t.Error("valids8 shoud not be nil")
	}
	if valids9 == nil {
		t.Error("valids9 shoud not be nil")
	}
	if valids10 == nil {
		t.Error("valids10 shoud not be nil")
	}
	if valids11 == nil {
		t.Error("valids11 shoud not be nil")
	}

	if errfull0 == nil {
		t.Error("error0 should not be nil")
	}
	if errfull1 == nil {
		t.Error("error1 should not be nil")
	}
	if errfull2 == nil {
		t.Error("error2 should not be nil")
	}
	if errfull3 == nil {
		t.Error("error3 should not be nil")
	}
	if errfull4 == nil {
		t.Error("error4 should not be nil")
	}
	if errfull5 == nil {
		t.Error("error5 should not be nil")
	}
	if errfull6 == nil {
		t.Error("error6 should not be nil")
	}
	if errfull7 == nil {
		t.Error("error7 should not be nil")
	}
	if errfull8 == nil {
		t.Error("error8 should not be nil")
	}
	if errfull9 == nil {
		t.Error("error9 should not be nil")
	}
	if errfull10 == nil {
		t.Error("error10 should not be nil")
	}
	if errfull11 == nil {
		t.Error("error11 should not be nil")
	}

	if full0 != nil {
		t.Error("full0 should be nil")
	}
	if full1 != nil {
		t.Error("full1 should be nil")
	}
	if full2 != nil {
		t.Error("full2 should be nil")
	}
	if full3 != nil {
		t.Error("full3 should be nil")
	}
	if full4 != nil {
		t.Error("full4 should be nil")
	}
	if full5 != nil {
		t.Error("full5 should be nil")
	}
	if full6 != nil {
		t.Error("full6 should be nil")
	}
	if full7 != nil {
		t.Error("full7 should be nil")
	}
	if full8 != nil {
		t.Error("full8 should be nil")
	}
	if full9 != nil {
		t.Error("full9 should be nil")
	}
	if full10 != nil {
		t.Error("error10full10 not nil")
	}
	if full11 != nil {
		t.Error("error11full11 not nil")
	}

	for ix := 0; ix < 10; ix++ {
		val0, isvalid0 := col0.Value(ix)
		val1, isvalid1 := col1.Value(ix)
		val2, isvalid2 := col2.Value(ix)
		val3, isvalid3 := col3.Value(ix)
		val4, isvalid4 := col4.Value(ix)
		val5, isvalid5 := col5.Value(ix)
		val6, isvalid6 := col6.Value(ix)
		val7, isvalid7 := col7.Value(ix)
		val8, isvalid8 := col8.Value(ix)
		val9, isvalid9 := col9.Value(ix)
		val10, isvalid10 := col10.Value(ix)
		val11, isvalid11 := col11.Value(ix)

		if want0[ix] != val0 || vals0[ix] != want0[ix] || isvalid0 != valids0[ix] {
			t.Errorf("Row %v, Col 0 error. want0[ix] = %v and val0 = %v and vals0[ix] = %v and isvalid0 = %v and valids0[ix] = %v", ix, want0[ix], val0, vals0[ix], isvalid0, valids0[ix])
		}
		if want1[ix] != val1 || vals1[ix] != want1[ix] || isvalid1 != valids1[ix] {
			t.Errorf("Row %v, Col 1 error. want1[ix] = %v and val1 = %v and vals1[ix] = %v and isvalid1 = %v and valids1[ix] = %v", ix, want1[ix], val1, vals1[ix], isvalid1, valids1[ix])
		}
		if want2[ix] != val2 || vals2[ix] != want2[ix] || isvalid2 != valids2[ix] {
			t.Errorf("Row %v, Col 2 error. want2[ix] = %v and val2 = %v and vals2[ix] = %v and isvalid2 = %v and valids2[ix] = %v", ix, want2[ix], val2, vals2[ix], isvalid2, valids2[ix])
		}
		if want3[ix] != val3 || vals3[ix] != want3[ix] || isvalid3 != valids3[ix] {
			t.Errorf("Row %v, Col 3 error. want3[ix] = %v and val3 = %v and vals3[ix] = %v and isvalid3 = %v and valids3[ix] = %v", ix, want3[ix], val3, vals3[ix], isvalid3, valids3[ix])
		}
		if want4[ix] != val4 || vals4[ix] != want4[ix] || isvalid4 != valids4[ix] {
			t.Errorf("Row %v, Col 4 error. want4[ix] = %v and val4 = %v and vals4[ix] = %v and isvalid4 = %v and valids4[ix] = %v", ix, want4[ix], val4, vals4[ix], isvalid4, valids4[ix])
		}
		if want5[ix] != val5 || vals5[ix] != want5[ix] || isvalid5 != valids5[ix] {
			t.Errorf("Row %v, Col 5 error. want5[ix] = %v and val5 = %v and vals5[ix] = %v and isvalid5 = %v and valids5[ix] = %v", ix, want5[ix], val5, vals5[ix], isvalid5, valids5[ix])
		}
		if want6[ix] != val6 || vals6[ix] != want6[ix] || isvalid6 != valids6[ix] {
			t.Errorf("Row %v, Col 6 error. want6[ix] = %v and val6 = %v and vals6[ix] = %v and isvalid6 = %v and valids6[ix] = %v", ix, want6[ix], val6, vals6[ix], isvalid6, valids6[ix])
		}
		if want7[ix] != val7 || vals7[ix] != want7[ix] || isvalid7 != valids7[ix] {
			t.Errorf("Row %v, Col 7 error. want7[ix] = %v and val7 = %v and vals7[ix] = %v and isvalid7 = %v and valids7[ix] = %v", ix, want7[ix], val7, vals7[ix], isvalid7, valids7[ix])
		}
		if want8[ix] != val8 || vals8[ix] != want8[ix] || isvalid8 != valids8[ix] {
			t.Errorf("Row %v, Col 8 error. want8[ix] = %v and val8 = %v and vals8[ix] = %v and isvalid8 = %v and valids8[ix] = %v", ix, want8[ix], val8, vals8[ix], isvalid8, valids8[ix])
		}
		if want9[ix] != val9 || vals9[ix] != want9[ix] || isvalid9 != valids9[ix] {
			t.Errorf("Row %v, Col 9 error. want9[ix] = %v and val9 = %v and vals9[ix] = %v and isvalid9 = %v and valids9[ix] = %v", ix, want9[ix], val9, vals9[ix], isvalid9, valids9[ix])
		}
		if want10[ix] != val10 || vals10[ix] != want10[ix] || isvalid10 != valids10[ix] {
			t.Errorf("Row %v, Col 10 error. want10[ix] = %v and val10 = %v and vals10[ix] = %v and isvalid10 = %v and valids10[ix] = %v", ix, want10[ix], val10, vals10[ix], isvalid10, valids10[ix])
		}
		if want11[ix] != val11 || vals11[ix] != want11[ix] || isvalid11 != valids11[ix] {
			t.Errorf("Row %v, Col 11 error. want11[ix] = %v and val11 = %v and vals11[ix] = %v and isvalid11 = %v and valids11[ix] = %v", ix, want11[ix], val11, vals11[ix], isvalid11, valids11[ix])
		}
	}
}

func TestMissingMagicBits(t *testing.T) {
	fns := []string{"test_data/missing_fea1_start.feather", "test_data/missing_fea1_end.feather"}
	for _, fn := range fns {
		_, err := Read(fn)
		if err == nil {
			t.Fail()
		}
	}
}

func TestNoFile(t *testing.T) {
	_, err := Read("foobar")
	if err == nil {
		t.Fail()
	}
}

func TestDictEncoding(t *testing.T) {
	fn := "test_data/cats.feather"
	src, err := Read(fn)
	if err != nil {
		t.Error("failed to read cats.feather")
	}
	if src.NumCols() != 2 {
		t.Error("Expected 2 columns in cats.feather")
	}

	if src.NumRows() != 10 {
		t.Error("Expected 10 columns in cats.feather")
	}

	want0 := []float32{1, 2, 3, 1, 1, 2, 2, 3, 3, 2}
	col0 := src.Columns[0].(*Float32Int32DictColumn)
	vals0, valids0 := col0.Values()
	want1 := []string{"a", "b", "c", "a", "a", "b", "b", "c", "c", "b"}
	col1 := src.Columns[1].(*StringInt32DictColumn)
	vals1, valids1 := col1.Values()

	if valids0 != nil {
		t.Error("Expected no nulls in column0, but valids0 is not null")
	}

	if valids1 != nil {
		t.Error("Expected no nulls in column0, but valids0 is not null")
	}

	for ix := 0; ix < src.NumRows(); ix++ {
		val0, _ := col0.Value(ix)
		if want0[ix] != val0 || vals0[ix] != want0[ix] {
			t.Errorf("In col 0, row %v expected %v  but found %v and %v", ix, want0[ix], val0, vals0[ix])
		}
		val1, _ := col1.Value(ix)
		if want1[ix] != val1 || vals1[ix] != want1[ix] {
			t.Errorf("In col 1, row %v expected %v  but found %v and %v", ix, want1[ix], val1, vals1[ix])
		}
	}
}

func TestDictEncodingMissing(t *testing.T) {
	fn := "test_data/cats_missing.feather"
	src, err := Read(fn)
	if err != nil {
		t.Error("failed to read cats.feather")
	}
	if src.NumCols() != 2 {
		t.Error("Expected 2 columns in cats.feather")
	}

	if src.NumRows() != 10 {
		t.Error("Expected 10 columns in cats.feather")
	}

	want0 := []float32{1, 2, 3, 1, 1, 2, 2, 3, 3, 0}
	actualValids0 := []bool{true, true, true, true, true, true, true, true, true, false}
	col0 := src.Columns[0].(*Float32Int32DictColumn)
	vals0, valids0 := col0.Values()

	want1 := []string{"a", "b", "c", "a", "a", "b", "b", "", "c", "b"}
	actualValids1 := []bool{true, true, true, true, true, true, true, false, true, true}
	col1 := src.Columns[1].(*StringInt32DictColumn)
	vals1, valids1 := col1.Values()

	for ix := 0; ix < src.NumRows(); ix++ {
		val0, valid0 := col0.Value(ix)
		if want0[ix] != val0 || vals0[ix] != want0[ix] {
			t.Errorf("In col 0, row %v expected %v  but found %v and %v", ix, want0[ix], val0, vals0[ix])
		}
		if valids0[ix] != actualValids0[ix] || actualValids0[ix] != valid0 {
			t.Errorf("In col 0, row %v expected valid to be %v, but found %v", ix, actualValids0[ix], valid0)
		}

		val1, valid1 := col1.Value(ix)
		if want1[ix] != val1 || vals1[ix] != want1[ix] {
			t.Errorf("In col 1, row %v expected %v  but found %v and %v", ix, want1[ix], val1, vals1[ix])
		}
		if valids1[ix] != actualValids1[ix] || actualValids1[ix] != valid1 {
			t.Errorf("In col 1, row %v expected valid to be %v, but found %v", ix, actualValids1[ix], valid1)
		}
	}
}

func TestNullIssue1(t *testing.T) {
	src, err := Read("test_data/testnull_issue1.feather")
	if err != nil {
		t.Error("Couldn't open testnull_issue1.feather")
	}
	col1 := src.Columns[1].(*Float64Column)
	vals1, valid1 := col1.Values()

	valsWant := []float64{181.9, 1600, math.NaN(), 192.5}
	validWant := []bool{true, true, false, true}

	for ix := range vals1 {
		if vals1[ix] != valsWant[ix] {
			t.Errorf("Expected %v and found %v on row %v", valsWant[ix], vals1[ix], ix)
		}

		if valid1[ix] != validWant[ix] {
			t.Errorf("Expected %v and found %v on row %v", validWant[ix], valid1[ix], ix)
		}
	}
}
