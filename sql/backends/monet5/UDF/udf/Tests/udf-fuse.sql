set optimizer = 'sequential_pipe';

create table udf_fuse ( a tinyint , b tinyint , c smallint , d smallint , e integer , f integer );
insert into udf_fuse values  (1,2,1000,2000,1000000,2000000);
insert into udf_fuse values  (3,4,3000,4000,3000000,4000000);
insert into udf_fuse values  (5,6,5000,6000,5000000,6000000);
insert into udf_fuse values  (7,8,7000,8000,7000000,8000000);
select * from udf_fuse;

create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

-- First scalar

call profiler."starttrace"();
select fuse(1,2);
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.fuse%'; -- don't use bulk
select count(*) from sys.tracelog() where stmt like '% udf.fuse%'; -- use scalar
select fuse(1,2);

call profiler."starttrace"();
select fuse(1000,2000);
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.fuse%'; -- don't use bulk
select count(*) from sys.tracelog() where stmt like '% udf.fuse%'; -- use scalar
select fuse(1000,2000);

call profiler."starttrace"();
select fuse(1000000,2000000);
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.fuse%'; -- don't use bulk
select count(*) from sys.tracelog() where stmt like '% udf.fuse%'; -- use scalar
select fuse(1000000,2000000);

-- Now into bulk versions

call profiler."starttrace"();
select fuse(a,b) from udf_fuse;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.fuse%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% udf.fuse%'; -- don't use scalar
select fuse(a,b) from udf_fuse;

call profiler."starttrace"();
select fuse(c,d) from udf_fuse;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.fuse%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% udf.fuse%'; -- don't use scalar
select fuse(c,d) from udf_fuse;

call profiler."starttrace"();
select fuse(e,f) from udf_fuse;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.fuse%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% udf.fuse%'; -- don't use scalar
select fuse(e,f) from udf_fuse;

drop procedure profiler.starttrace();
drop procedure profiler.stoptrace();
drop table udf_fuse;

set optimizer = 'default_pipe';
