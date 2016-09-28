package indirectedarray

import (
	"fmt"
	. "github.com/tpch-hand-coded-golang/reader"
	"sort"
)

type KeyMap []int16
func (km KeyMap) Len() int { return len(km) }
func (km KeyMap) Swap(i, j int) { km[i], km[j] = km[j], km[i] }
func (km KeyMap) Less(i, j int) bool { return km[i] < km[j] }

func AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	fr := i_fr.(*ResultSet)
	for _, key  := range partialResult.Keymap {
		aggs := partialResult.Data[key]
		if fr.Data[key] == nil {
			fr.Data[key] = make([]float64, 8)
			fr.Keymap = append(fr.Keymap,key)
		}
		for ii := 0; ii < 8; ii++ {
			fr.Data[key][ii] += aggs[ii]
		}
	}
}

func FinalizeResultSet(i_partialResult interface{}) {
	partialResult := i_partialResult.(*ResultSet)
	for _, key  := range partialResult.Keymap {
		for ii := 4; ii < 7; ii++ {
			partialResult.Data[key][ii] /= partialResult.Data[key][7]
		}
	}
}

func PrintResultSet(i_fr interface{}) {
	fr := i_fr.(*ResultSet)
	sort.Sort(fr.Keymap)
	for _, key  := range fr.Keymap {
		aggs := fr.Data[key]
		res1 := key>>8
		res2 := (key<<8)>>8
		fmt.Printf("%c ", byte(res1))
		fmt.Printf("%c ", byte(res2))
		fmt.Printf("%10d ", int(aggs[0]))
		for i := 1; i < 4; i++ {
			fmt.Printf("%15.2f ", aggs[i])
		}
		for i := 4; i < 7; i++ {
			fmt.Printf("%7.2f ", aggs[i])
		}
		fmt.Printf("%10d ", int(aggs[7]))
		fmt.Printf("\n")
	}
}

type ResultSet struct {
	/*
	 outer: number of groups
	 middle: number of potential key values in group, i.e. 256 for 8-bit cardinality
	 last: aggregates
	  */
	Data   [][]float64
	/*
	array of key values found, one for each group
	 */
	Keymap KeyMap
}

func NewResultSet(buckets int) *ResultSet {
	rs := new(ResultSet)
	rs.Data = make([][]float64,65536)
	rs.Keymap = make([]int16,0)
	return rs
}

func RunPart (rowData []LineItemRow, i_fr interface{}, nRows int) {
	fr := i_fr.(*ResultSet)
	for i:=0; i<nRows; i++ {
		row := rowData[i]
		if row.L_shipdate <= DatePredicate {
			res1 := row.L_returnflag
			res2 := row.L_linestatus
			key := int16(res1)<<8 + int16(res2)
			if fr.Data[key] == nil {
				fr.Data[key] = make([]float64, 8)
				fr.Keymap = append(fr.Keymap, key)
			}
			fr.Data[key][0] += row.L_quantity
			fr.Data[key][1] += row.L_extendedprice
			fr.Data[key][2] += row.L_extendedprice * (1. - row.L_discount)
			fr.Data[key][3] += row.L_extendedprice * (1. - row.L_discount) * (1. + row.L_tax)
			fr.Data[key][4] += row.L_quantity
			fr.Data[key][5] += row.L_extendedprice
			fr.Data[key][6] += row.L_discount
			fr.Data[key][7]++ //count
		}
	}
}
