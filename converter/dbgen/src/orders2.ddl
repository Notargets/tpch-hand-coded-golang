CREATE TABLE ORDERS2  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     FLOAT8 NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  TEXT NOT NULL,  -- R
                           O_CLERK          TEXT NOT NULL,  -- R
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        TEXT NOT NULL);
