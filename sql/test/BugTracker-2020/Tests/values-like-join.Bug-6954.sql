create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

call profiler."starttrace"();
select 1 from (values ('one'), ('two')) as l(s), (values ('three'), ('four')) as r(s) where l.s like r.s;
call profiler.stoptrace();
select count(*) from sys.tracelog() where stmt like '%cross%'; -- no crossproducts

drop procedure profiler.starttrace();
drop procedure profiler.stoptrace();

select 1 from (values ('one'), ('two')) as l(s), (values ('three'), ('four')) as r(s) where l.s like r.s;
