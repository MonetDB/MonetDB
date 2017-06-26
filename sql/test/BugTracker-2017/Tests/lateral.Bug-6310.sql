
CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL,
                          PRIMARY KEY   (P_PARTKEY) );

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL,
                             PRIMARY KEY    (PS_PARTKEY,PS_SUPPKEY),
                             FOREIGN KEY (PS_PARTKEY) references PART );

select
  ref_0.message as c0
from
  sys.rejects as ref_0,
  lateral (select
		(select name from sys.objects)
		   as c0
	  from
		sys.args as sample_1
		  right join tmp._columns as ref_4
			  inner join sys.part as ref_6
				  inner join sys.types as sample_5
				  on (ref_6.p_comment = sample_5.systemname )
				right join sys.netcdf_vars as ref_7
				on (sample_5.digits = ref_7.var_id )
			  on (ref_4.name = sample_5.systemname )
			right join sys.user_role as sample_8
			  inner join sys.partsupp as ref_10
			  on (sample_8.role_id = ref_10.ps_partkey )
			on (ref_4.name = ref_10.ps_comment )
		  on (sample_1.name = ref_10.ps_comment )
	  ) as subq_0;
