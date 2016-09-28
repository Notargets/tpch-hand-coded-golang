# tpch-hand-coded-golang
### Highest Performance execution of TPCH type queries

![Scaling Performance Result](https://github.com/llonergan/tpch-hand-coded-golang/blob/master/images/scaling-tpchq1-golang.PNG)

This is a testing ground for high performance execution ideas. The language is "go" and assumes go version 1.6 or better.

### Current status:
    - Initial implementation in pure golang for TPC-H query 1
    - Achieves approximately 200x speedup over Postgres V9 for 1GB of TPC-H data
    - Parallelized using Golang goroutines

### Current roadmap:
    - Improve parallel speedup - find current threading bottleneck
    - Implement GPU based processing for the kernel using CGO and cuda

### Usage:
    - First you need to generate and convert a TPC-H lineitem data table. Do this:
        cd converter; make
        go run converter.go
        mv lineitem.bin ..
        cd ..

    - Now you can run the tpchq1 test:
        go run tpchtest.go -maxProcs 4

**Note** the code limits the number of goroutines to the number of CPU cores you have on your machine.

