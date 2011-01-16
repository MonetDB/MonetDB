create table tmp_one (data numeric(5));
create table tmp_two (data numeric(5));

/* this is ok */
select data from tmp_one
union
select data from tmp_two
;

/* this will crash server */
select data from tmp_one
union
select data from tmp_two
order by data
;

/* work-around */
select *
from (
  select data from tmp_one
  union
  select data from tmp_two
) as foo
order by data
;

drop table tmp_one;
drop table tmp_two;
