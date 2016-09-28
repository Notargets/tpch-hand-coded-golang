# tpch-hand-coded-golang
### Target: Highest Performance execution of TPCH type queries

This is a testing ground for high performance execution ideas. The language is "go" and assumes go version 1.6 or better.

### Results
![Scaling Performance Result](https://github.com/llonergan/tpch-hand-coded-golang/blob/master/images/scaling-tpchq1-golang.PNG)

```
Q1 Execution Time (including IO) =   1.33s
Ran with: An array based aggregator with a max of 256 unique values per grouping attribute
A F   37734107  56586554400.73  53758257134.87  55909065222.83   25.52 38273.13    0.05    1478493
N F     991417   1487504710.38   1413082168.05   1469649223.19   25.52 38284.47    0.05      38854
N O   73112824 109655889346.67 104174442729.04 108345127221.17   25.50 38248.15    0.05    2866959
R F   37719753  56568041380.90  53741292684.60  55889619119.83   25.51 38250.85    0.05    1478870
```

Postgres Result:
```
 l_returnflag | l_linestatus | sum_qty  |  sum_base_price  |  sum_disc_price  |    sum_charge    |     avg_qty      |    avg_price     |      avg_disc      | count_order
--------------+--------------+----------+------------------+------------------+------------------+------------------+------------------+--------------------+-------------
 A            | F            | 37734107 | 56586554400.7299 | 53758257134.8651 | 55909065222.8256 | 25.5220058532573 | 38273.1297346216 | 0.0499852958382544 |     1478493
 N            | F            |   991417 |    1487504710.38 |  1413082168.0541 | 1469649223.19436 |  25.516471920523 | 38284.4677608482 | 0.0500934266741932 |       38854
 N            | O            | 73112824 | 109655889346.674 |  104174442729.04 | 108345127221.171 | 25.5018728904041 | 38248.1540010423 | 0.0499980257826594 |     2866959
 R            | F            | 37719753 | 56568041380.9045 | 53741292684.6038 | 55889619119.8297 | 25.5057936126908 | 38250.8546261027 | 0.0500094058299836 |     1478870
```


### Current status:
    - Initial implementation in pure golang for TPC-H query 1
    - Parallelized using Golang goroutines

### Current roadmap:
    - Improve parallel speedup - find current bottleneck
    - Use CGO to implement kernel processing to compare execution speed with golang
    - Vectorize chunks before execution

### Usage:
    - First you need to generate and convert a TPC-H lineitem data table. Do this:
        cd converter; make
        go run converter.go
        mv lineitem.bin ..
        cd ..

    - Now you can run the tpchq1 test:
        go run tpchtest-all.go -RunAll -maxProcs 10

**Note** the code limits the number of goroutines to the number of CPU cores you have on your machine.

