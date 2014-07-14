set optimizer = 'sequential_pipe';

explain select fuse(1,2);
select fuse(1,2);

explain select fuse(1000,2000);
select fuse(1000,2000);

explain select fuse(1000000,2000000);
select fuse(1000000,2000000);

create table udf_fuse ( a tinyint , b tinyint , c smallint , d smallint , e integer , f integer );
insert into udf_fuse values  (1,2,1000,2000,1000000,2000000);
insert into udf_fuse values  (3,4,3000,4000,3000000,4000000);
insert into udf_fuse values  (5,6,5000,6000,5000000,6000000);
insert into udf_fuse values  (7,8,7000,8000,7000000,8000000);
select * from udf_fuse;

explain select fuse(a,b) from udf_fuse;
explain select fuse(c,d) from udf_fuse;
explain select fuse(e,f) from udf_fuse;
select fuse(a,b) from udf_fuse;
select fuse(c,d) from udf_fuse;
select fuse(e,f) from udf_fuse;
