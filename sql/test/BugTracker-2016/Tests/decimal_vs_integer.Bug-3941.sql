set optimizer = 'sequential_pipe';

start transaction;
create table tmp(i decimal(8));

create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

call profiler."starttrace"();
select count(*) from tmp where i = 20160222;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '%batcalc.int%' or stmt like '%batcalc.lng%'; --no conversions
select count(*) from tmp where i = 20160222;

call profiler."starttrace"();
select count(*) from tmp where i = '20160222';
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '%batcalc.int%' or stmt like '%batcalc.lng%'; --no conversions
select count(*) from tmp where i = '20160222';

call profiler."starttrace"();
select count(*) from tmp where i = 201602221;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '%batcalc.int%'; -- convert to int
select count(*) from sys.tracelog() where stmt like '%batcalc.lng%'; -- no conversion to lng
select count(*) from tmp where i = 201602221;

rollback;
set optimizer = 'default_pipe';
