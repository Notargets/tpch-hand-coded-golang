\timing

DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS  (
			   O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     FLOAT8 NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  TEXT NOT NULL,  -- R
                           O_CLERK          TEXT NOT NULL,  -- R
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        TEXT NOT NULL)
			   WITH (appendonly=true,compresstype=quicklz,compresslevel=3,blocksize=32768);

DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM ( 
			     L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    FLOAT8 NOT NULL,
                             L_EXTENDEDPRICE  FLOAT8 NOT NULL,
                             L_DISCOUNT    FLOAT8 NOT NULL,
                             L_TAX         FLOAT8 NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT TEXT NOT NULL,  -- R
                             L_SHIPMODE     TEXT NOT NULL,  -- R
                             L_COMMENT      TEXT NOT NULL)
			     WITH (appendonly=true,compresstype=quicklz,compresslevel=3,blocksize=32768);

insert into orders (select
			   O_ORDERKEY,
                           O_CUSTKEY,
                           O_ORDERSTATUS,
                           O_TOTALPRICE,
                           O_ORDERDATE,
                           O_ORDERPRIORITY,
                           O_CLERK,
                           O_SHIPPRIORITY,
                           O_COMMENT
			   FROM orders_orig
			   order by o_orderdate
			   );

insert into lineitem (select
			     L_ORDERKEY,
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
                             L_COMMENT
			   FROM lineitem_orig
			   order by l_shipdate
			   );

ANALYZE ORDERS;

ANALYZE LINEITEM;
