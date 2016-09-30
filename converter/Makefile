lineitem.bin: lineitem.tbl
	go run converter.go
lineitem.tbl:
	bash -c "cd dbgen/src && make >& build-output.txt && ./dbgen -T L && mv lineitem.tbl ../.."
