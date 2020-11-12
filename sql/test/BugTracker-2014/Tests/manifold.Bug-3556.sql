set optimizer = 'sequential_pipe';

create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

call profiler."starttrace"();
select replace(a1,a2,a3), id
from (
  select name as a1, 'a' as a2, 'A' as a3, id as id 
  from (values (1, '1'), (2, '2')) as x(id, name)
) as x;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batstr.replace%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% str.replace%'; -- don't use scalar

call profiler."starttrace"();
select replace(a1,a2,a3), id
from (
  select name as a1, 'a' as a2, 'A' as a3, id + 1 as id 
  from (values (1, '1'), (2, '2')) as x(id, name)
) as x;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batstr.replace%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% str.replace%'; -- don't use scalar

call profiler."starttrace"();
select replace(a1,a2,a3), id
from (
  select name as a1, 'a' as a2, 'A' as a3, abs(id) as id 
 from (values (1, '1'), (2, '2')) as x(id, name)
) as x;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% batstr.replace%'; -- use bulk
select count(*) from sys.tracelog() where stmt like '% str.replace%'; -- don't use scalar

drop procedure profiler.starttrace();
drop procedure profiler.stoptrace();

set optimizer = 'default_pipe';
