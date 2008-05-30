create table timestamp1 (
nr integer auto_increment,
tr_time timestamp(3) with time zone
)
;
insert into timestamp1 (tr_time)
select *
from
(
select timestamp with time zone '2005-04-12 06:30-07:00' as tr_time
union
select timestamp with time zone '2005-04-12 07:30-07:00' as tr_time
union
select timestamp with time zone '2005-04-12 05:30-07:00' as tr_time
union
select timestamp with time zone '2005-04-12 05:35-07:00' as tr_time
union
select timestamp with time zone '2005-04-12 07:15-07:00' as tr_time
) as foo
;
-- this will crash server
select nr from timestamp1
order by tr_time
limit 3
;

drop table timestamp1;
