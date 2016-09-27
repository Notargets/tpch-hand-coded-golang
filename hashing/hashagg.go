package hashing

import (
	"fmt"
	. "github.com/tpch-hand-coded-golang/reader"
)

func AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	fr := i_fr.(*ResultSet)
	for res1, map2 := range partialResult.Data {
		if fr.Data[res1] == nil {
			fr.Data[res1] = make(singleHashMap, 256)
		}
		for res2, aggs := range map2 {
			if fr.Data[res1][res2] == nil {
				fr.Data[res1][res2] = make([]float64, 8)
			}
			for ii := 0; ii < 8; ii++ {
				fr.Data[res1][res2][ii] += aggs[ii]
			}
		}
	}
}

func FinalizeResultSet(i_partialResult interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	for res1, map2 := range partialResult.Data {
		for res2 := range map2 {
			for ii := 4; ii < 7; ii++ {
				partialResult.Data[res1][res2][ii] /= partialResult.Data[res1][res2][7]
			}
		}
	}
}

func PrintResultSet(i_fr interface{}) {
	fr := i_fr.(*ResultSet)
	for res1, map2 := range fr.Data {
		for res2 := range map2 {
			fmt.Printf("%c ", byte(res1))
			fmt.Printf("%c ", byte(res2))
			fmt.Printf("%10d ", int(fr.Data[res1][res2][0]))
			for i := 1; i < 4; i++ {
				fmt.Printf("%15.2f ", fr.Data[res1][res2][i])
			}
			for i := 4; i < 7; i++ {
				fmt.Printf("%7.2f ", fr.Data[res1][res2][i])
			}
			fmt.Printf("%10d ", int(fr.Data[res1][res2][7]))
			fmt.Printf("\n")
		}
	}
}

type doubleHashMap map[byte]map[byte][]float64
type singleHashMap map[byte][]float64

type ResultSet struct {
	/*
	 outer: number of groups
	 middle: number of potential key values in group, i.e. 256 for 8-bit cardinality
	 last: aggregates
	  */
	// TODO: make this a single level hash table using a compound 16-bit key (concat the two 8-bits together)
	Data doubleHashMap
}

func NewResultSet() *ResultSet {
	rs := new(ResultSet)
	rs.Data = make(doubleHashMap, 256)
	return rs
}

func RunPart (rowData []LineItemRow, i_fr interface{}, nRows int) {
	fr := i_fr.(*ResultSet)
	for i:=0; i<nRows; i++ {
		row := rowData[i]
		if row.L_shipdate <= DatePredicate {
			res1 := row.L_returnflag
			res2 := row.L_linestatus
			if fr.Data[res1] == nil {
				fr.Data[res1] = make(singleHashMap, 256)
			}
			if fr.Data[res1][res2] == nil {
				fr.Data[res1][res2] = make([]float64, 8)
			}
			fr.Data[res1][res2][0] += row.L_quantity
			fr.Data[res1][res2][1] += row.L_extendedprice
			fr.Data[res1][res2][2] += row.L_extendedprice * (1. - row.L_discount)
			fr.Data[res1][res2][3] += row.L_extendedprice * (1. - row.L_discount) * (1. + row.L_tax)
			fr.Data[res1][res2][4] += row.L_quantity
			fr.Data[res1][res2][5] += row.L_extendedprice
			fr.Data[res1][res2][6] += row.L_discount
			fr.Data[res1][res2][7]++ //count
		}
	}
}
