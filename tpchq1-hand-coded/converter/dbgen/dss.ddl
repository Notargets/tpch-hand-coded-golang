-- Sccsid:     @(#)dss.ddl	2.1.8.1
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       TEXT NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    TEXT) WITH (appendonly=true,orientation=column);

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       TEXT NOT NULL,
                            R_COMMENT    TEXT) WITH (appendonly=true,orientation=column);

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        TEXT NOT NULL,
                          P_MFGR        TEXT NOT NULL,
                          P_BRAND       TEXT NOT NULL,
                          P_TYPE        TEXT NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   TEXT NOT NULL,
                          P_RETAILPRICE FLOAT8 NOT NULL,
                          P_COMMENT     TEXT NOT NULL ) WITH (appendonly=true,orientation=column);

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        TEXT NOT NULL,
                             S_ADDRESS     TEXT NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       TEXT NOT NULL,
                             S_ACCTBAL     FLOAT8 NOT NULL,
                             S_COMMENT     TEXT NOT NULL) WITH (appendonly=true,orientation=column);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  FLOAT8  NOT NULL,
                             PS_COMMENT     TEXT NOT NULL ) WITH (appendonly=true,orientation=column);

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        TEXT NOT NULL,
                             C_ADDRESS     TEXT NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       TEXT NOT NULL,
                             C_ACCTBAL     FLOAT8   NOT NULL,
                             C_MKTSEGMENT  TEXT NOT NULL,
                             C_COMMENT     TEXT NOT NULL) WITH (appendonly=true,orientation=column);

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     FLOAT8 NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  TEXT NOT NULL,  -- R
                           O_CLERK          TEXT NOT NULL,  -- R
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        TEXT NOT NULL) WITH (appendonly=true,orientation=column);

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
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
                             L_COMMENT      TEXT NOT NULL) WITH (appendonly=true,orientation=column);

