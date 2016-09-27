package executor

import (
	. "github.com/tpch-hand-coded-golang/reader"
)

type Executor interface {
	PrintableDescription() string
	NewResultSet() interface{}
	RunPart([]LineItemRow, interface {}, int)
	AccumulateResultSet(interface {}, interface {})
	FinalizeResultSet(interface {})
	PrintResultSet(interface {})
}
