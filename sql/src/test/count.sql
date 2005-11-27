start transaction;
create table test (val int );
insert into test values (10);
insert into test values (20);
insert into test values (30);
insert into test values (10);
insert into test values (20);
insert into test values (NULL);
select count(*) from test;
-- 6
select count(val) from test;
-- 5
select count(*) as count_x from test group by val order by count_x;
--        1 
--        1 
--        2 
--        2 
select count(val) as count_val from test group by val order by count_val;
--        0 
--        1 
--        2 
--        2 
drop table test;

commit;
