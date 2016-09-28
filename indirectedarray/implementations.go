package indirectedarray

import (
	"reflect"
	"github.com/tpch-hand-coded-golang/executor"
	. "github.com/tpch-hand-coded-golang/reader"
)

const baseDescription = "An indirected array based aggregator with a max of "

type Q1HashAgg struct {
	executor.Executor
}
func NewExecutor() *Q1HashAgg {
	return new(Q1HashAgg)
}
func (e Q1HashAgg) PrintableDescription() string {
	return baseDescription + "256 unique values per grouping attribute"
}
func (e Q1HashAgg) NewResultSet() interface{} {
	rs := NewResultSet(65536)
	return reflect.ValueOf(rs).Interface()
}
func (e Q1HashAgg) RunPart (rowData []LineItemRow, i_fr interface{}, nRows int) {
	RunPart(rowData, i_fr, nRows)
}
func (e Q1HashAgg) AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	AccumulateResultSet(i_partialResult, i_fr)
}
func (e Q1HashAgg) FinalizeResultSet(i_partialResult interface{}) {
	FinalizeResultSet(i_partialResult)
}
func (e Q1HashAgg) PrintResultSet(i_fr interface{}) {
	PrintResultSet(i_fr)
}
