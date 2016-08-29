package main

import (
	"bufio"
	"fmt"
	"os"
	. "reflect"
	"strconv"
	"time"
	"unsafe"
)

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
type LineItemRow struct {
	L_orderkey, L_partkey, L_suppkey, L_linenumber int64
	L_quantity, L_extendedprice, L_discount, L_tax float64
	L_shipdate, L_commitdate, L_receiptdate        int64
	L_returnflag, L_linestatus                     byte
	// Variable length structures at the end
	L_shipinstruct, L_shipmode, L_comment string
}

func main() {
	splitAtChar := func(data []byte, splitChar byte) (nextIndex int, token []byte, isEnd bool) {
		for i, char := range data {
			if char == splitChar {
				return i + 1, data[:i], false
			}
		}
		return 0, data, true
	}

	file, err := os.Open("lineitem.tbl")
	if err != nil {
		fmt.Println("Error opening file")
		os.Exit(1)
	}

	// Read the lineitem table file in CSV format
	startTime := time.Now()
	var rowNum int
	lineitem1GB := make([]LineItemRow, 0, 7000000)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rowData := scanner.Bytes()
		lineitem1GB = append(lineitem1GB, LineItemRow{})
		li := &lineitem1GB[rowNum]

		nextIndex := 0
		var columnData []byte
		isEnd := false
		column := int8(0)
		for {
			if isEnd {
				break
			}
			nextIndex, columnData, isEnd = splitAtChar(rowData, '|')
			rowData = rowData[nextIndex:]
			columnString := string(columnData)
			var intVar int
			var int64Var int64
			var float64Var float64
			var byteVar byte
			if column < 4 {
				intVar, _ = strconv.Atoi(columnString)
				int64Var = int64(intVar)
			} else if column < 8 {
				float64Var, _ = strconv.ParseFloat(columnString, 64)
			} else if column < 10 {
				byteVar = columnData[0]
			} else if column < 13 {
				timeVar, _ := time.Parse("2006-01-02", columnString)
				int64Var = timeVar.Unix()
			}
			switch column {
			case 0:
				li.L_orderkey = int64Var
			case 1:
				li.L_partkey = int64Var
			case 2:
				li.L_suppkey = int64Var
			case 3:
				li.L_linenumber = int64Var
			case 4:
				li.L_quantity = float64Var
			case 5:
				li.L_extendedprice = float64Var
			case 6:
				li.L_discount = float64Var
			case 7:
				li.L_tax = float64Var
			case 8:
				li.L_returnflag = byteVar
			case 9:
				li.L_linestatus = byteVar
			case 10:
				li.L_shipdate = int64Var
			case 11:
				li.L_commitdate = int64Var
			case 12:
				li.L_receiptdate = int64Var
			case 13:
				li.L_shipinstruct = columnString
			case 14:
				li.L_shipmode = columnString
			case 15:
				li.L_comment = columnString
			}
			column++
		}
		rowNum++
	}
	fmt.Printf("Reading the input file took %6.2fs for %v rows...(sooo slow!)\n", time.Since(startTime).Seconds(), rowNum)

	outputFile, err := os.OpenFile("lineitem.bin", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// We write 10000 rows at a time to allow for parallel IO on consumption
	chunkSize := 10000
	buffer := []byte{}
	var rowCount int64
	for i, row := range lineitem1GB {
		buffer, _ = Serialize(buffer, row)
		rowCount++
		if (i+1)%chunkSize == 0 || (i+1) == len(lineitem1GB) {
			metaBuffer, _ := Serialize([]byte{}, rowCount)
			metaBuffer, _ = Serialize(metaBuffer, int64(len(buffer)))
			outputFile.Write(metaBuffer)
			outputFile.Write(buffer)
			buffer = []byte{}
			rowCount = 0
		}
	}
}

// This is a *copy* of the "Value" struct inside the reflect package
type MValue struct {
	typ uintptr
	Ptr unsafe.Pointer
}

// Takes a primary (non slice, non pointer) type and returns a []byte of the base type data
func DataToByteSlice(srcData interface{}) []byte {
	value := ValueOf(srcData)
	size := int(value.Type().Size())
	buffer := make([]byte, size, size)
	(*SliceHeader)(unsafe.Pointer(&buffer)).Data =
		uintptr(unsafe.Pointer((*(*MValue)(unsafe.Pointer(&value))).Ptr))
	return buffer
}

//Serializes various primitive types into a byte representation, useful for output to files
func Serialize(buffer []byte, datum interface{}) ([]byte, error) {
	if buffer == nil {
		buffer = make([]byte, 0)
	}
	value := ValueOf(datum)
	var err error
	switch value.Kind() {
	case Chan, Func, Interface, Ptr, UnsafePointer:
		return buffer, fmt.Errorf("Serialize: Type %s is not serializable", value.Kind().String())
	case String:
		strLen := len(datum.(string))
		/*
		 We allow for serialization of strings > int16 in length
		 - If the string is larger than int16, we use the special value 32767 to indicate
		   that there is a following int64 that specifies the true length
		*/
		if strLen > 32766 {
			buffer, _ = Serialize(buffer, int16(32767))
			buffer, _ = Serialize(buffer, int64(strLen))
		} else {
			buffer, _ = Serialize(buffer, int16(strLen))
		}
		return append(buffer, datum.(string)[:strLen]...), nil
	case Struct:
		for i := 0; i < value.NumField(); i++ {
			subDatum := value.Field(i).Interface()
			buffer, err = Serialize(buffer, subDatum)
			if err != nil {
				return nil, err
			}
		}
		return buffer, nil
	case Slice:
	case Array:
		for i := 0; i < value.Len(); i++ {
			buffer, err = Serialize(buffer, value.Index(i).Interface())
			if err != nil {
				return nil, err
			}
		}
		return buffer, nil
	case Map:
		for _, key := range value.MapKeys() {
			// We serialize the key length, then the key string, then the value
			buffer, err = Serialize(buffer, int16(key.Len()))
			if err != nil {
				return nil, err
			}
			buffer, err = Serialize(buffer, key.Interface())
			if err != nil {
				return nil, err
			}
			buffer, err = Serialize(buffer, value.MapIndex(key).Interface())
			if err != nil {
				return nil, err
			}
		}
		return buffer, nil
	}
	return append(buffer, DataToByteSlice(datum)...), nil
}
