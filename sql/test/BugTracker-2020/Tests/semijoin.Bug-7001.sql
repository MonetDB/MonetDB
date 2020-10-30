create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

create table i as select * from (VALUES (1),(2),(3)) as x(n);

call profiler."starttrace"();

with
t as (select 1 as r, * from sys.functions where name in ('sum', 'prod'))
select distinct name from t where r in (select n from i);

call profiler.stoptrace();
-- no crossproducts
select count(*) from sys.tracelog() where stmt like '%cross%';

drop table i;
