LOAD NATION  ( N_NATIONKEY,
                            N_NAME,
                            N_REGIONKEY,
                            N_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/nation.tbl' WITH DELIMITER '|';

LOAD REGION  ( R_REGIONKEY,
                            R_NAME,
                            R_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/region.tbl' WITH DELIMITER '|';

LOAD PART  ( P_PARTKEY,
                          P_NAME,
                          P_MFGR,
                          P_BRAND,
                          P_TYPE,
                          P_SIZE,
                          P_CONTAINER,
                          P_RETAILPRICE,
                          P_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/part.tbl' WITH DELIMITER '|';

LOAD SUPPLIER ( S_SUPPKEY,
                             S_NAME,
                             S_ADDRESS,
                             S_NATIONKEY,
                             S_PHONE,
                             S_ACCTBAL,
                             S_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/supplier.tbl' WITH DELIMITER '|';

LOAD PARTSUPP ( PS_PARTKEY,
                             PS_SUPPKEY,
                             PS_AVAILQTY,
                             PS_SUPPLYCOST,
                             PS_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/partsupp.tbl' WITH DELIMITER '|';

LOAD CUSTOMER ( C_CUSTKEY,
                             C_NAME,
                             C_ADDRESS,
                             C_NATIONKEY,
                             C_PHONE,
                             C_ACCTBAL,
                             C_MKTSEGMENT,
                             C_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/customer.tbl' WITH DELIMITER '|';

LOAD ORDERS  ( O_ORDERKEY,
                           O_CUSTKEY,
                           O_ORDERSTATUS,
                           O_TOTALPRICE,
                           O_ORDERDATE,
                           O_ORDERPRIORITY,  
                           O_CLERK,  
                           O_SHIPPRIORITY,
                           O_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/orders.tbl' WITH DELIMITER '|';

LOAD LINEITEM  ( L_ORDERKEY,
                             L_PARTKEY,
                             L_SUPPKEY,
                             L_LINENUMBER,
                             L_QUANTITY,
                             L_EXTENDEDPRICE,
                             L_DISCOUNT,
                             L_TAX,
                             L_RETURNFLAG,
                             L_LINESTATUS,
                             L_SHIPDATE,
                             L_COMMITDATE,
                             L_RECEIPTDATE,
                             L_SHIPINSTRUCT,  
                             L_SHIPMODE,  
                             L_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/lineitem.tbl' WITH DELIMITER '|';
