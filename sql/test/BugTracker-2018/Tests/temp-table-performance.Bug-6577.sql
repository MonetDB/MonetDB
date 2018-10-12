create or replace function test1() RETURNS TABLE (v0 INT, v1 INT)
LANGUAGE PYTHON {
	import numpy as np
	nrows=10000000
	i_var=np.random.randint(1,1000000, (10000000,))
	return [[i for i in range(nrows)],i_var]
};

create table t1 as (select * from test1()) with data;
create table t2 as (select distinct v1 from t1) with data;

select count(t1.v1) from t1, t2 where t1.v1=t2.v1;

create temp table t3 as (select count(t1.v1) from t1, t2 where t1.v1=t2.v1) on commit preserve rows;
drop table t3;

create temp table t3 as (select t1.v1 from t1, t2 where t1.v1=t2.v1) on commit preserve rows;
drop table t3;

drop function test1;
drop table t1;
drop table t2;
