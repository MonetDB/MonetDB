create table test_6722  (part string, qty double);
insert into test_6722 values ('a', 18), ('a', 13),('a', 16),('b', 15),('b', 16),('c', 17),('c', 18),('c', 12),('d', 12),('d', 12);

select
 dense_rank() over (order by part, qty) as rank_id, 
 row_number() over (order by part, qty) as row_id,                       
 *
from test_6722
order by part, qty;

select
 (dense_rank() over (order by part, qty)) as rank_id, 
 *,
 (row_number() over (order by part, qty)) as row_id 
from test_6722
order by part, qty;

select
 *,
 (dense_rank() over (order by part, qty)) as rank_id, 
 (row_number() over (order by part, qty)) as row_id 
from test_6722
order by part, qty;

drop table test_6722;

