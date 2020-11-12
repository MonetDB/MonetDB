set optimizer = 'sequential_pipe';

create table udf_reverse ( x string );
insert into udf_reverse values ('MonetDB');
insert into udf_reverse values ('Database Architecture');
insert into udf_reverse values ('Information Systems');
insert into udf_reverse values ('Centrum Wiskunde & Informatica');
select * from udf_reverse;

create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

-- First scalar

call profiler."starttrace"();
select reverse('MonetDB');
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.reverse%'; -- don't use bulk
select count(*) from sys.tracelog() where stmt like '% udf.reverse%'; -- use scalar
select reverse('MonetDB');

-- Now into bulk version

call profiler."starttrace"();
select reverse(x) from udf_reverse;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batudf.reverse%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% udf.reverse%'; -- don't use scalar
select reverse(x) from udf_reverse;

drop procedure profiler.starttrace();
drop procedure profiler.stoptrace();

drop table udf_reverse;

set optimizer = 'default_pipe';
