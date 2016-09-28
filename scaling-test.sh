#!/bin/bash

EXE=tpchtest-all

rm $EXE
go build $EXE.go

for procs in 1 2 4 6 8 10 20
do
    printf "Running %d threads...\n" $procs
    ./$EXE -RunAll -maxProcs $procs
    #./$EXE -workerStats -RunAll -maxProcs $procs
done

