package reader

import (
	"fmt"
	"os"
	"unsafe"
)

var DatePredicate int64

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
	L_orderkey, L_partkey, L_suppkey, L_linenumber int64
	L_quantity, L_extendedprice, L_discount, L_tax float64
	L_shipdate, L_commitdate, L_receiptdate        int64
	L_returnflag, L_linestatus                     byte
}
type LineItemRowVariable struct {
	L_shipinstruct, L_shipmode, L_comment MyString
}

func ToInt64(b []byte) int64 {
	return *(*int64)(unsafe.Pointer(&b[0]))
}

type DataChunk struct {
	Nrows int
	Data  []byte
}

func ParallelReader(fileName string, chunkChannel chan DataChunk) {
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
		chunkChannel <- DataChunk{Nrows: int(nRows), Data: inputBuffer}
		//	fmt.Printf("Read a buffer of size %d with %d rows\n", n, nRows)
	}
}
