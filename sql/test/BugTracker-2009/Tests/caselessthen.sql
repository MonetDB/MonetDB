create table tcase(i int);
insert into tcase values(10);
insert into tcase values(11);
insert into tcase values(12);
insert into tcase values(13);
select x
, case when x<=12 then 1 else 0 end as log 
from (
  select 10 as x 
  union 
  select 11 as x 
  union 
  select 12 as x 
  union 
  select 13 as x
) sub
;
drop table tcase;
