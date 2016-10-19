START TRANSACTION;

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);

select  
  ref_188.type as c0, 
  ref_222.p_container as c1, 
  ref_188.schema as c2
from 
  	sys.netcdf_vardim as ref_140
  	left join bam.sq as ref_182
  	left join tmp.triggers as ref_183
  	left join sys.querylog_history as ref_184
  	inner join sys.users as ref_185 on (ref_184.mal = ref_185.default_schema )
		on (ref_183.table_id = ref_185.default_schema )
		on (ref_182.file_id = ref_184.optimize )
  	right join tmp.objects as ref_186
		on (ref_182.ln = ref_186.id )
		on (ref_140.var_id = ref_185.default_schema )
  	left join sys.systemfunctions as ref_187
		on (ref_140.dim_id = ref_187.function_id )
  	left join sys.storage as ref_188
  	left join sys.part as ref_222
	left join sys.types as ref_227
	inner join sys.lineitem as ref_228
		on (ref_227.radix = ref_228.l_orderkey )
		on (ref_222.p_type = ref_228.l_comment )
	inner join sys.storage as ref_229
		on (ref_227.eclass = ref_229.typewidth )
	inner join sys.netcdf_files as ref_230
	inner join sys.querylog_catalog as ref_231
		on (ref_230.file_id = ref_231.mal )
	inner join tmp.objects as ref_232
		on (ref_230.file_id = ref_232.id )
	left join sys.storagemodelinput as ref_233
		on (ref_230.file_id = ref_233.typewidth )
	right join sys.storage as ref_234
		on (ref_232.nr = ref_234.typewidth )
		on (ref_229.columnsize = ref_234.count )
		on (ref_188.sorted = ref_233.reference )
		on (ref_186.id = ref_188.typewidth )
	inner join bam.export as ref_238
		on (ref_140.var_id = ref_238.pos )
where ref_231.pipe is NULL;

ROLLBACK;
