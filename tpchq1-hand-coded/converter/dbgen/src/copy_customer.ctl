COPY CUSTOMER ( C_CUSTKEY,
                             C_NAME,
                             C_ADDRESS,
                             C_NATIONKEY,
                             C_PHONE,
                             C_ACCTBAL,
                             C_MKTSEGMENT,
                             C_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/customer.tbl' WITH DELIMITER '|';
