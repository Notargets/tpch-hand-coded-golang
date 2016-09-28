#!/bin/bash

EXE=tpchtest-all

if [ ! -x $EXE ]; then
    go build $EXE.go
fi

for procs in 1 2 4 6 8 10
do
    printf "Running %d threads...\n" $procs
    ./$EXE -RunArray -RunIndirect -RunHashAgg -maxProcs $procs
done

