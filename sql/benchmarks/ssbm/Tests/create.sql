create table DWDATE (
D_DATEKEY int, --identifier, unique id -- e.g. 19980327 (what we use)
D_DATE string, --varchar(18), --fixed text, size 18, longest: December 22, 1998
D_DAYOFWEEK string, --varchar(8), --fixed text, size 8, Sunday, Monday, ..., Saturday)
D_MONTH string, --varchar(9), --fixed text, size 9: January, ..., December
D_YEAR int, --unique value 1992-1998
D_YEARMONTHNUM int, --numeric (YYYYMM) -- e.g. 199803
D_YEARMONTH string, --varchar(7), --fixed text, size 7: Mar1998 for example
D_DAYNUMINWEEK int, --numeric 1-7
D_DAYNUMINMONTH int, --numeric 1-31
D_DAYNUMINYEAR int, --numeric 1-366
D_MONTHNUMINYEAR int, --numeric 1-12
D_WEEKNUMINYEAR int, --numeric 1-53
D_SELLINGSEASON string, --varchar(12), --text, size 12 (Christmas, Summer,...)
D_LASTDAYINWEEKFL int, --1 bit
D_LASTDAYINMONTHFL int, --1 bit
D_HOLIDAYFL int, -- 1 bit
D_WEEKDAYFL int, --1 bit
primary key(D_DATEKEY) --Primary Key: DATEKEY
);

create table SUPPLIER (
S_SUPPKEY int, --identifier
S_NAME string, --varchar(25), --fixed text, size 25: 'Supplier'||SUPPKEY
S_ADDRESS string, --varchar(25), --variable text, size 25 (city below)
S_CITY string, --varchar(10), --fixed text, size 10 (10/nation: nation_prefix||(0-9))
S_NATION string, --varchar(15), --fixed text(15) (25 values, longest UNITED KINGDOM)
S_REGION string, --varchar(12), --fixed text, size 12 (5 values: longest MIDDLE EAST)
S_PHONE string, --varchar(15) --fixed text, size 15 (many values, format: 43-617-354-1222)
primary key(S_SUPPKEY) --Primary Key: SUPPKEY
);

create table CUSTOMER (
C_CUSTKEY int, --numeric identifier
C_NAME string, --varchar(25), --variable text, size 25 'Customer'||CUSTKEY
C_ADDRESS string, --varchar(25), --variable text, size 25 (city below)
C_CITY string, --varchar(10), --fixed text, size 10 (10/nation: NATION_PREFIX||(0-9)
C_NATION string, --varchar(15), --fixed text(15) (25 values, longest UNITED KINGDOM)
C_REGION string, --varchar(12), --fixed text, size 12 (5 values: longest MIDDLE EAST)
C_PHONE string, --varchar(15), --fixed text, size 15 (many values, format: 43-617-354-1222)
C_MKTSEGMENT string, --varchar(10) --fixed text, size 10 (longest is AUTOMOBILE)
primary key (C_CUSTKEY) --Primary Key: CUSTKEY
);

create table PART (
P_PARTKEY int, --identifier
P_NAME string, --varchar(22), --variable text, size 22 (Not unique per PART but never was)
P_MFGR string, --varchar(6), --fixed text, size 6 (MFGR#1-5, CARD = 5)
P_CATEGORY string, --varchar(7), --fixed text, size 7 ('MFGR#'||1-5||1-5: CARD = 25)
P_BRAND1 string, --varchar(9), --fixed text, size 9 (CATEGORY||1-40: CARD = 1000)
P_COLOR string, --varchar(11), --variable text, size 11 (CARD = 94)
P_TYPE string, --varchar(25), --variable text, size 25 (CARD = 150)
P_SIZE int, --numeric 1-50 (CARD = 50)
P_CONTAINER string, --varchar(15) --fixed text(10) (CARD = 40)
primary key (P_PARTKEY) --Primary Key: PARTKEY
);
 
create table LINEORDER (
LO_ORDERKEY int, --numeric (int up to SF 300) first 8 of each 32 keys used
LO_LINENUMBER int, -- numeric 1-7
LO_CUSTKEY int, -- numeric identifier foreign key reference to C_CUSTKEY
LO_PARTKEY int, --identifier foreign key reference to P_PARTKEY
LO_SUPPKEY int, --numeric identifier foreign key reference to S_SUPPKEY
LO_ORDERDATE int, --identifier foreign key reference to D_DATEKEY
LO_ORDERPRIORITY string, --varchar(15), --fixed text, size 15 (5 Priorities: 1-URGENT, etc.)
LO_SHIPPRIORITY string, --varchar(1), --fixed text, size 1
LO_QUANTITY int, -- numeric 1-50 (for PART)
LO_EXTENDEDPRICE int, --numeric, MAX about 55,450 (for PART)
LO_ORDTOTALPRICE int, --numeric, MAX about 388,000 (for ORDER)
LO_DISCOUNT int, --numeric 0-10 (for PART) -- (Represents PERCENT)
LO_REVENUE int, --numeric (for PART: (extendedprice*(100-discount))/100)
LO_SUPPLYCOST int, --numeric (for PART, cost from supplier, max = ?)
LO_TAX int, -- numeric 0-8 (for PART)
LO_COMMITDATE int, --Foreign Key reference to D_DATEKEY
LO_SHIPMODE string, --varchar(10) --fixed text, size 10 (Modes: REG AIR, AIR, etc.)
primary key (LO_ORDERKEY, LO_LINENUMBER), --Compound Primary Key: ORDERKEY, LINENUMBER
FOREIGN KEY (LO_ORDERDATE)  REFERENCES DWDATE   (D_DATEKEY), --identifier foreign key reference to D_DATEKEY
FOREIGN KEY (LO_COMMITDATE) REFERENCES DWDATE   (D_DATEKEY), --Foreign Key reference to D_DATEKEY
FOREIGN KEY (LO_SUPPKEY)    REFERENCES SUPPLIER (S_SUPPKEY), --numeric identifier foreign key reference to S_SUPPKEY
FOREIGN KEY (LO_CUSTKEY)    REFERENCES CUSTOMER (C_CUSTKEY), --numeric identifier foreign key reference to C_CUSTKEY
FOREIGN KEY (LO_PARTKEY)    REFERENCES PART     (P_PARTKEY)  --identifier foreign key reference to P_PARTKEY
);
