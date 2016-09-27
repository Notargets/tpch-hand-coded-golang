package indirectedarray

import (
	"reflect"
	"github.com/tpch-hand-coded-golang/executor"
	. "github.com/tpch-hand-coded-golang/reader"
)

const baseDescription = "An indirected array based aggregator with a max of "

type Q1HashAgg8 struct {
	executor.Executor
}
func NewExecutor8() *Q1HashAgg8 {
	return new(Q1HashAgg8)
}
func (e Q1HashAgg8) PrintableDescription() string {
	return baseDescription + "256 unique values"
}
func (e Q1HashAgg8) NewResultSet() interface{} {
	rs := NewResultSet(256)
	return reflect.ValueOf(rs).Interface()
}
func (e Q1HashAgg8) RunPart (rowData []LineItemRow, i_fr interface{}, nRows int) {
	RunPart(rowData, i_fr, nRows)
}
func (e Q1HashAgg8) AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	AccumulateResultSet(i_partialResult, i_fr)
}
func (e Q1HashAgg8) FinalizeResultSet(i_partialResult interface{}) {
	FinalizeResultSet(i_partialResult)
}
func (e Q1HashAgg8) PrintResultSet(i_fr interface{}) {
	PrintResultSet(i_fr)
}

type Q1HashAgg16 struct {
	executor.Executor
}
func NewExecutor16() *Q1HashAgg16 {
	return new(Q1HashAgg16)
}
func (e Q1HashAgg16) PrintableDescription() string {
	return baseDescription + "65536 unique values"
}
func (e Q1HashAgg16) NewResultSet() interface{} {
	rs := NewResultSet(65536)
	return reflect.ValueOf(rs).Interface()
}
func (e Q1HashAgg16) RunPart (rowData []LineItemRow, i_fr interface{}, nRows int) {
	RunPart(rowData, i_fr, nRows)
}
func (e Q1HashAgg16) AccumulateResultSet(i_partialResult interface{}, i_fr interface{}) {
	AccumulateResultSet(i_partialResult, i_fr)
}
func (e Q1HashAgg16) FinalizeResultSet(i_partialResult interface{}) {
	FinalizeResultSet(i_partialResult)
}
func (e Q1HashAgg16) PrintResultSet(i_fr interface{}) {
	PrintResultSet(i_fr)
}
