package array

import (
	"fmt"
	. "github.com/tpch-hand-coded-golang/reader"
)

func AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	fr := i_fr.(*ResultSet)
	for i, Map := range partialResult.Data {
		for res, val := range Map {
			if val != nil {
				if fr.Data[i][res] == nil {
					fr.Data[i][res] = make([]float64, 10)
				}
				fr.Data[i][res][0] = val[0]
				fr.Data[i][res][1] = val[1]
				for ii := 2; ii < 10; ii++ {
					fr.Data[i][res][ii] += val[ii]
				}
			}
		}
	}
}

func FinalizeResultSet(i_partialResult interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	for _, Map := range partialResult.Data {
		for _, val := range Map {
			if val != nil {
				for ii := 6; ii < 9; ii++ {
					val[ii] /= val[9]
				}
			}
		}
	}
}

func PrintResultSet(i_fr interface{}) {
	fr := i_fr.(*ResultSet)
	for _, Map := range fr.Data {
		for _, val := range Map {
			if val != nil {
				for i := 0; i < 2; i++ {
					fmt.Printf("%c ", byte(val[i]))
				}
				fmt.Printf("%10d ", int(val[2]))
				for i := 3; i < 6; i++ {
					fmt.Printf("%15.2f ", val[i])
				}
				for i := 6; i < 9; i++ {
					fmt.Printf("%7.2f ", val[i])
				}
				fmt.Printf("%10d ", int(val[9]))
				fmt.Printf("\n")
			}
		}
	}
}

type ResultSet struct {
	/*
	 outer: number of groups
	 middle: number of potential key values in group, i.e. 256 for 8-bit cardinality
	 last: aggregates
	  */
	Data   [][][]float64
}

func NewResultSet(buckets int) *ResultSet {
	rs := new(ResultSet)
	rs.Data = make([][][]float64,2)
	rs.Data[0] = make([][]float64, buckets)
	rs.Data[1] = make([][]float64, buckets)
	return rs
}

func RunPart (rowData []LineItemRow, i_fr interface{}, nRows int) {
//func RunPart (rowData []LineItemRow, i_fr interface{}) {
	fr := i_fr.(*ResultSet)
	for i:=0; i<nRows; i++ {
		row := rowData[i]
//	for _, row := range rowData {
		if row.L_shipdate <= DatePredicate {
			res1 := row.L_returnflag
			res2 := row.L_linestatus
			if fr.Data[0][res1] == nil {
				fr.Data[0][res1] = make([]float64, 10)
			}
			if fr.Data[1][res2] == nil {
				fr.Data[1][res2] = make([]float64, 10)
			}
			fr.Data[0][res1][0] = float64(res1)
			fr.Data[0][res1][1] = float64(res2)
			fr.Data[0][res1][2] += row.L_quantity
			fr.Data[0][res1][3] += row.L_extendedprice
			fr.Data[0][res1][4] += row.L_extendedprice * (1. - row.L_discount)
			fr.Data[0][res1][5] += row.L_extendedprice * (1. - row.L_discount) * (1. + row.L_tax)
			fr.Data[0][res1][6] += row.L_quantity
			fr.Data[0][res1][7] += row.L_extendedprice
			fr.Data[0][res1][8] += row.L_discount
			fr.Data[0][res1][9]++ //count

			fr.Data[1][res2][0] = float64(res1)
			fr.Data[1][res2][1] = float64(res2)
			fr.Data[1][res2][2] += row.L_quantity
			fr.Data[1][res2][3] += row.L_extendedprice
			fr.Data[1][res2][4] += row.L_extendedprice * (1. - row.L_discount)
			fr.Data[1][res2][5] += row.L_extendedprice * (1. - row.L_discount) * (1. + row.L_tax)
			fr.Data[1][res2][6] += row.L_quantity
			fr.Data[1][res2][7] += row.L_extendedprice
			fr.Data[1][res2][8] += row.L_discount
			fr.Data[1][res2][9]++ //count
		}
	}
}
