package main

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"
	"github.com/tpch-hand-coded-golang/array"
	"github.com/tpch-hand-coded-golang/hashing"
	. "github.com/tpch-hand-coded-golang/reader"
	"github.com/tpch-hand-coded-golang/executor"
	"github.com/tpch-hand-coded-golang/indirectedarray"
)

var maxProcs, forceProcs int
var RunSetup struct {
	RunAll, RunArray, RunHashagg, RunIndirect bool
}
var workerStats bool

func init() {
	flag.IntVar(&maxProcs, "maxProcs", 1024, "Maximum number of parallel processes to launch")
	flag.IntVar(&forceProcs, "forceProcs", 0, "Required number of parallel processes to launch")
	flag.BoolVar(&workerStats,"workerStats", false, "Print out stats for each worker")
	flag.BoolVar(&RunSetup.RunArray,"RunArray", false, "Run 16 bit precision array test")
	flag.BoolVar(&RunSetup.RunHashagg, "RunHashAgg", false, "Run HashAgg test")
	flag.BoolVar(&RunSetup.RunIndirect, "RunIndirect", false, "Run Indirect for array tests")
	flag.BoolVar(&RunSetup.RunAll, "RunAll", false, "Run all available tests")
	flag.Parse()
	if forceProcs != 0 {
		fmt.Printf("Forced number of processes set to: %d\n", forceProcs)
	} else {
		fmt.Printf("Maximum number of processes set to: %d\n", maxProcs)
	}
	parsed, _ := time.Parse("2006-01-02", "1998-12-01")
	DatePredicate = parsed.AddDate(0, 0, -115).Unix()

	if RunSetup.RunAll {
		RunSetup.RunArray = true
		RunSetup.RunIndirect = true
		RunSetup.RunHashagg = true
	}
}

// From the DDL in the TPC-H benchmark directory:
/*
CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    FLOAT8 NOT NULL,
                             L_EXTENDEDPRICE  FLOAT8 NOT NULL,
                             L_DISCOUNT    FLOAT8 NOT NULL,
                             L_TAX         FLOAT8 NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT TEXT NOT NULL,  -- R
                             L_SHIPMODE     TEXT NOT NULL,  -- R
                             L_COMMENT      TEXT NOT NULL) WITH (appendonly=true,orientation=column);

*/

func main() {
	/*
	   select
	   	L_returnflag,
	   	L_linestatus,
	   	sum(L_quantity) as sum_qty,
	   	sum(L_extendedprice) as sum_base_price,
	   	sum(L_extendedprice * (1 - L_discount)) as sum_disc_price,
	   	sum(L_extendedprice * (1 - L_discount) * (1 + L_tax)) as sum_charge,
	   	avg(L_quantity) as avg_qty,
	   	avg(L_extendedprice) as avg_price,
	   	avg(L_discount) as avg_disc,
	   	count(*) as count_order
	   from
	   	lineitem
	   where
	   	L_shipdate <= date '1998-12-01' - interval '115 day'
	   group by
	   	L_returnflag,
	   	L_linestatus
	   order by
	   	L_returnflag,
	   	L_linestatus;
	*/
	/*
	 We need a map of columns and of grouping buckets, because both are simple ordinal sets we'll use slices
	 	- The first slice level is the grouping bucket
	 	- The second slice level is the result column number
	*/

	if RunSetup.RunArray {
		RunQuery(array.NewExecutor())
	}
	if RunSetup.RunIndirect {
		RunQuery(indirectedarray.NewExecutor())
	}
	if RunSetup.RunHashagg {
		RunQuery(hashing.NewExecutor())
	}
}

func RunQuery(executor executor.Executor)	{
	numGoRoutines := MaxParallelism(maxProcs)
	if forceProcs != 0 {
		numGoRoutines = forceProcs
	}

	/*
	---------------------- Begin query processing -------------------------
	 */
	startTime := time.Now()

	/*
	Startup the read thread - reads data in the background
	 */
	maxThreads := numGoRoutines
	if maxThreads < 12 {
		maxThreads = 12
	}
	chunkChannel := make(chan DataChunk, maxThreads)
	// Spin up the async reader
	go ParallelReader("lineitem.bin", chunkChannel)

	/*
	Process the query in parallel
	 */
	wg := new(sync.WaitGroup)
	resultChannel := make(chan interface{}, numGoRoutines+1)
	for threadID := 0; threadID < numGoRoutines; threadID++ {
//		fmt.Printf("Starting thread# %d\n", threadID)
		wg.Add(1)
		go ProcessByStrips(executor, resultChannel, chunkChannel, wg)
	}
	wg.Wait()

	fullResult := executor.NewResultSet()
	for i := 0; i < numGoRoutines; i++ {
		result := <-resultChannel
		executor.AccumulateResultSet(result, fullResult)
	}
	executor.FinalizeResultSet(fullResult)

	/*
	------------------- Query processing is finished ----------------------
	 */
	duration := time.Since(startTime)
	fmt.Printf("Q1 Execution Time (including IO) = %6.2fs\n", duration.Seconds())

	fmt.Printf("Ran with: %s\n", executor.PrintableDescription())
	executor.PrintResultSet(fullResult)
}

func MaxParallelism(limiter int) (nLimit int) {
	nLimit = limiter
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < nLimit {
		nLimit = maxProcs
	}
	if numCPU < nLimit {
		nLimit = numCPU
	}
	return nLimit
}


func ProcessByStrips(ex executor.Executor, resultChan chan interface{}, chunkChannel chan DataChunk, wg *sync.WaitGroup) {
	var rowCount int
	var ioTime, calcTime float64
	defer func() {
		wg.Done()
		if workerStats {
			fmt.Printf("Row Count = %d, IOtime = %5.3fs, CalcTime = %5.3fs\n", rowCount, ioTime, calcTime)
		}
	}()

	fr := ex.NewResultSet()
	Q1HashAgg := ex.RunPart

	var lineitem1GBAligned []LineItemRow
	var lineitem1GBVariable []LineItemRowVariable
	readLineItemData := func(chunk DataChunk) (li []LineItemRow, liv []LineItemRowVariable, nRows int) {
		if len(lineitem1GBAligned) < chunk.Nrows {
			lineitem1GBAligned = make([]LineItemRow, chunk.Nrows)
			lineitem1GBVariable = make([]LineItemRowVariable, chunk.Nrows)
		}
		castToRow := func(b []byte) *LineItemRow {
			return (*LineItemRow)(unsafe.Pointer(&b[0]))
		}
		var cursor int
		for i := 0; i < chunk.Nrows; i++ {
			lineitem1GBAligned[i] = *(castToRow(chunk.Data[cursor:]))
			cursor += 90

			liv := &lineitem1GBVariable[i]

			liv.L_shipinstruct.SetLen(chunk.Data[cursor:])
			cursor += 2

			strlen := int(liv.L_shipinstruct.Len)
			liv.L_shipinstruct.SetData(chunk.Data[cursor : cursor+strlen])
			cursor += strlen

			liv.L_shipmode.SetLen(chunk.Data[cursor:])
			cursor += 2

			strlen = int(liv.L_shipmode.Len)
			liv.L_shipmode.SetData(chunk.Data[cursor : cursor+strlen])
			cursor += strlen

			liv.L_comment.SetLen(chunk.Data[cursor:])
			cursor += 2

			strlen = int(liv.L_comment.Len)
			liv.L_comment.SetData(chunk.Data[cursor : cursor+strlen])
			cursor += strlen
		}
		return lineitem1GBAligned, lineitem1GBVariable, chunk.Nrows
	}

	for {
		startTime := time.Now()
		chunk, open := <-chunkChannel
		if !open {
			break
		}
		li, _, nRows := readLineItemData(chunk)
		ioTimePartial := time.Since(startTime)
		rowCount += nRows
		Q1HashAgg(li, fr, nRows)
		calcTimePartial := time.Since(startTime.Add(ioTimePartial))
		calcTime += calcTimePartial.Seconds()
		ioTime += ioTimePartial.Seconds()
	}

	resultChan <- fr
}
