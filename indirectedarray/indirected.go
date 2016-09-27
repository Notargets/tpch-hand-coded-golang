package indirectedarray

import (
	"fmt"
	. "github.com/tpch-hand-coded-golang/reader"
)

func AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	fr := i_fr.(*ResultSet)
	for i, Map := range partialResult.Data {
		for _, key := range partialResult.Keymap[i] {
			if fr.Data[i][key] == nil {
				fr.Data[i][key] = make([]float64, 10)
				//				fmt.Printf("New key[%d] = %d\n",ii,key)
				fr.Keymap[i] = append(fr.Keymap[i], key)
			}
			fr.Data[i][key][0] = Map[key][0]
			fr.Data[i][key][1] = Map[key][1]
			for ii := 2; ii < 10; ii++ {
				fr.Data[i][key][ii] += Map[key][ii]
			}
		}
	}
}

func FinalizeResultSet(i_partialResult interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	for i, Map := range partialResult.Data {
		for _, key := range partialResult.Keymap[i] {
			for ii := 6; ii < 9; ii++ {
				Map[key][ii] /= Map[key][9]
			}
		}
	}
}

func PrintResultSet(i_fr interface{}) {
	fr := i_fr.(*ResultSet)
	for i, Map := range fr.Data {
		for _, key := range fr.Keymap[i] {
			for ii := 0; ii < 2; ii++ {
				fmt.Printf("%c ", byte(Map[key][ii]))
			}
			fmt.Printf("%10d ", int(Map[key][2]))
			for ii := 3; ii < 6; ii++ {
				fmt.Printf("%15.2f ", Map[key][ii])
			}
			for ii := 6; ii < 9; ii++ {
				fmt.Printf("%7.2f ", Map[key][ii])
			}
			fmt.Printf("%10d ", int(Map[key][9]))
			fmt.Printf("\n")
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
	/*
	array of key values found, one for each group
	 */
	Keymap [][]int
}

func NewResultSet(buckets int) *ResultSet {
	rs := new(ResultSet)
	rs.Data = make([][][]float64,2)
	rs.Data[0] = make([][]float64, buckets)
	rs.Data[1] = make([][]float64, buckets)
	rs.Keymap = make([][]int,2)
	return rs
}

func RunPart (rowData []LineItemRow, i_fr interface{}) {
	fr := i_fr.(*ResultSet)
	for _, row := range rowData {
		if row.L_shipdate <= DatePredicate {
			res1 := row.L_returnflag
			res2 := row.L_linestatus
			if fr.Data[0][res1] == nil {
				fr.Data[0][res1] = make([]float64, 10)
				fr.Keymap[0] = append(fr.Keymap[0], int(res1))
			}
			if fr.Data[1][res2] == nil {
				fr.Data[1][res2] = make([]float64, 10)
				fr.Keymap[1] = append(fr.Keymap[1], int(res2))
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
