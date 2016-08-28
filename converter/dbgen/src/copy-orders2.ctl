COPY ORDERS2  ( O_ORDERKEY,
                           O_CUSTKEY,
                           O_ORDERSTATUS,
                           O_TOTALPRICE,
                           O_ORDERDATE,
                           O_ORDERPRIORITY,  -- R
                           O_CLERK,  -- R
                           O_SHIPPRIORITY,
                           O_COMMENT) FROM '/dbfast/TPCH/appendix/dbgen/orders.tbl' WITH DELIMITER '|';
