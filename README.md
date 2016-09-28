# tpch-hand-coded-golang
### Target: Highest Performance execution of TPCH type queries

This is a testing ground for high performance execution ideas. The language is "go" and assumes go version 1.6 or better.

### Results
![Scaling Performance Result](https://github.com/llonergan/tpch-hand-coded-golang/blob/master/images/scaling-tpchq1-golang.PNG)

Q1 Execution Time (including IO) =   0.57s
Ran with: An array based aggregator with a max of 256 unique values per grouping attribute
A F   37734107  56586554400.73  53758257134.87  55909065222.83   25.52 38273.13    0.05    1478493
N F     991417   1487504710.38   1413082168.05   1469649223.19   25.52 38284.47    0.05      38854
N O   73112824 109655889346.68 104174442729.04 108345127221.17   25.50 38248.15    0.05    2866959
R F   37719753  56568041380.90  53741292684.60  55889619119.83   25.51 38250.85    0.05    1478870


### Current status:
    - Initial implementation in pure golang for TPC-H query 1
    - Parallelized using Golang goroutines

### Current roadmap:
    - Improve parallel speedup - find current bottleneck
    - Use CGO to implement kernel processing to compare execution speed with golang

### Usage:
    - First you need to generate and convert a TPC-H lineitem data table. Do this:
        cd converter; make
        go run converter.go
        mv lineitem.bin ..
        cd ..

    - Now you can run the tpchq1 test:
        go run tpchtest-all.go -RunAll -maxProcs 10

**Note** the code limits the number of goroutines to the number of CPU cores you have on your machine.

