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
select count(*) from test group by val;
--        1 
--        2 
--        2 
--        1 
select count(val) from test group by val;
--        0 
--        2 
--        2 
--        1 
drop table test;

commit;
