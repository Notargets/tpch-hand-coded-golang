package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

var maxProcs, forceProcs int
var datePredicate int64

func init() {
	flag.IntVar(&maxProcs, "maxProcs", 1024, "Maximum number of parallel processes to launch")
	flag.IntVar(&forceProcs, "forceProcs", 0, "Required number of parallel processes to launch")
	flag.Parse()
	if forceProcs != 0 {
		fmt.Printf("Forced number of processes set to: %d\n", forceProcs)
	} else {
		fmt.Printf("Maximum number of processes set to: %d\n", maxProcs)
	}
	parsed, _ := time.Parse("2006-01-02", "1998-12-01")
	datePredicate = parsed.AddDate(0, 0, -115).Unix()
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
type MyString struct {
	Len  int16
	Data []byte
}

func (ms *MyString) SetLen(b []byte) {
	ms.Len = *(*int16)(unsafe.Pointer(&b[0]))
}
func (ms *MyString) SetData(b []byte) {
	ms.Data = b
}

type LineItemRow struct {
	l_orderkey, l_partkey, l_suppkey, l_linenumber int64
	l_quantity, l_extendedprice, l_discount, l_tax float64
	l_shipdate, l_commitdate, l_receiptdate        int64
	l_returnflag, l_linestatus                     byte
}
type LineItemRowVariable struct {
	l_shipinstruct, l_shipmode, l_comment MyString
}

func main() {
	/*
	   select
	   	l_returnflag,
	   	l_linestatus,
	   	sum(l_quantity) as sum_qty,
	   	sum(l_extendedprice) as sum_base_price,
	   	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	   	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	   	avg(l_quantity) as avg_qty,
	   	avg(l_extendedprice) as avg_price,
	   	avg(l_discount) as avg_disc,
	   	count(*) as count_order
	   from
	   	lineitem
	   where
	   	l_shipdate <= date '1998-12-01' - interval '115 day'
	   group by
	   	l_returnflag,
	   	l_linestatus
	   order by
	   	l_returnflag,
	   	l_linestatus;
	*/
	/*
	 We need a map of columns and of grouping buckets, because both are simple ordinal sets we'll use slices
	 	- The first slice level is the grouping bucket
	 	- The second slice level is the result column number
	*/

	numGoRoutines := MaxParallelism(maxProcs)
	if forceProcs != 0 {
		numGoRoutines = forceProcs
	}

	wg := new(sync.WaitGroup)
	startTime := time.Now()
	//chunkChannel := make(chan Chunk, 100*(numGoRoutines+1))
	chunkChannel := make(chan Chunk, 10*(numGoRoutines+1))
	// Spin up the async reader
	go parallelReader("lineitem.bin", chunkChannel)

	resultChannel := make(chan [][][]float64, numGoRoutines+1)
	for threadID := 0; threadID < numGoRoutines; threadID++ {
		fmt.Printf("Starting thread# %d\n", threadID)
		wg.Add(1)
		go ProcessByStrips(resultChannel, chunkChannel, threadID, numGoRoutines, wg)
	}
	wg.Wait()

	fullResult := make([][][]float64,2)
	fullResult[0] = make([][]float64, 65535)
	fullResult[1] = make([][]float64, 65535)
	for i := 0; i < numGoRoutines; i++ {
		result := <-resultChannel
		fullResult = AccumulateResultSet(result, fullResult)
	}
	fullResult = FinalizeResultSet(fullResult)

	duration := time.Since(startTime)
	fmt.Printf("Q1 Execution Time (including IO) = %6.2fs\n", duration.Seconds())
	for _, Map := range fullResult {
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

func ToInt64(b []byte) int64 {
	return *(*int64)(unsafe.Pointer(&b[0]))
}


func ProcessByStrips(resultChan chan [][][]float64, chunkChannel chan Chunk, threadID, numberOfThreads int, wg *sync.WaitGroup) {
	var rowCount int
	var ioTime, calcTime float64
	defer func() {
		wg.Done()
		fmt.Printf("Row Count = %d, IOtime = %5.3fs, CalcTime = %5.3fs\n", rowCount, ioTime, calcTime)
	}()

	fr := make([][][]float64,2)
	fr[0] = make([][]float64, 65535)
	fr[1] = make([][]float64, 65535)

	Q1HashAgg := func(rowData []LineItemRow) {
		for _, row := range rowData {
			if row.l_shipdate <= datePredicate {
				res1 := row.l_returnflag
				res2 := row.l_linestatus
				if fr[0][res1] == nil {
					fr[0][res1] = make([]float64, 10)
				}
				if fr[1][res2] == nil {
					fr[1][res2] = make([]float64, 10)
				}
				fr[0][res1][0] = float64(res1)
				fr[0][res1][1] = float64(res2)
				fr[0][res1][2] += row.l_quantity
				fr[0][res1][3] += row.l_extendedprice
				fr[0][res1][4] += row.l_extendedprice * (1. - row.l_discount)
				fr[0][res1][5] += row.l_extendedprice * (1. - row.l_discount) * (1. + row.l_tax)
				fr[0][res1][6] += row.l_quantity
				fr[0][res1][7] += row.l_extendedprice
				fr[0][res1][8] += row.l_discount
				fr[0][res1][9]++ //count

				fr[1][res2][0] = float64(res1)
				fr[1][res2][1] = float64(res2)
				fr[1][res2][2] += row.l_quantity
				fr[1][res2][3] += row.l_extendedprice
				fr[1][res2][4] += row.l_extendedprice * (1. - row.l_discount)
				fr[1][res2][5] += row.l_extendedprice * (1. - row.l_discount) * (1. + row.l_tax)
				fr[1][res2][6] += row.l_quantity
				fr[1][res2][7] += row.l_extendedprice
				fr[1][res2][8] += row.l_discount
				fr[1][res2][9]++ //count
			}
		}
	}

	readLineItemData := func(chunk Chunk) (li []LineItemRow, liv []LineItemRowVariable) {
		lineitem1GBAligned := make([]LineItemRow, chunk.Nrows)
		lineitem1GBVariable := make([]LineItemRowVariable, chunk.Nrows)
		castToRow := func(b []byte) *LineItemRow {
			return (*LineItemRow)(unsafe.Pointer(&b[0]))
		}
		var cursor int
		for i := 0; i < chunk.Nrows; i++ {
			lineitem1GBAligned[i] = *(castToRow(chunk.Data[cursor:]))
			cursor += 90

			liv := &lineitem1GBVariable[i]

			liv.l_shipinstruct.SetLen(chunk.Data[cursor:])
			cursor += 2

			strlen := int(liv.l_shipinstruct.Len)
			liv.l_shipinstruct.SetData(chunk.Data[cursor : cursor+strlen])
			cursor += strlen

			liv.l_shipmode.SetLen(chunk.Data[cursor:])
			cursor += 2

			strlen = int(liv.l_shipmode.Len)
			liv.l_shipmode.SetData(chunk.Data[cursor : cursor+strlen])
			cursor += strlen

			liv.l_comment.SetLen(chunk.Data[cursor:])
			cursor += 2

			strlen = int(liv.l_comment.Len)
			liv.l_comment.SetData(chunk.Data[cursor : cursor+strlen])
			cursor += strlen
		}
		return lineitem1GBAligned, lineitem1GBVariable
	}

	for {
		startTime := time.Now()
		chunk, open := <-chunkChannel
		if !open {
			break
		}
		li, _ := readLineItemData(chunk)
		ioTimePartial := time.Since(startTime)
		rowCount += len(li)
		Q1HashAgg(li)
		calcTimePartial := time.Since(startTime.Add(ioTimePartial))
		calcTime += calcTimePartial.Seconds()
		ioTime += ioTimePartial.Seconds()
	}
	resultChan <- fr
}

type Chunk struct {
	Nrows int
	Data  []byte
}

func parallelReader(fileName string, chunkChannel chan Chunk) {
	inputFile, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("Error opening input file")
	}
	// Read the metadata for this chunk
	metaBuffer := make([]byte, 16)

	readMeta := func() (numLines, size int64, err error) {
		n, err := inputFile.Read(metaBuffer)
		if n != 16 || err != nil {
			return 0, 0, fmt.Errorf("End of file")
		}
		return ToInt64(metaBuffer), ToInt64(metaBuffer[8:]), nil
	}

	for {
		nRows, size, err := readMeta()
		if err != nil {
			close(chunkChannel)
			break
		}

		inputBuffer := make([]byte, size)
		n, err := inputFile.Read(inputBuffer)
		if n != len(inputBuffer) || err != nil {
			fmt.Println("Error reading input file")
			os.Exit(1)
		}
		chunkChannel <- Chunk{Nrows: int(nRows), Data: inputBuffer}
		//	fmt.Printf("Read a buffer of size %d with %d rows\n", n, nRows)
	}
}

func AccumulateResultSet(partialResult [][][]float64, fr [][][]float64) [][][]float64 {
	for i, Map := range partialResult {
		for res, val := range Map {
			if val != nil {
				if fr[i][res] == nil {
					fr[i][res] = make([]float64, 10)
				}
				fr[i][res][0] = val[0]
				fr[i][res][1] = val[1]
				for ii := 2; ii < 10; ii++ {
					fr[i][res][ii] += val[ii]
				}
			}
		}
	}
	return fr
}

func FinalizeResultSet(partialResult [][][]float64) [][][]float64 {
	for _, Map := range partialResult {
		for _, val := range Map {
			if val != nil {
				for ii := 6; ii < 9; ii++ {
					val[ii] /= val[9]
				}
			}
		}
	}
	return partialResult
}
