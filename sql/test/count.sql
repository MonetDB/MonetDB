start transaction;
create table counttest (val int );
insert into counttest values (10);
insert into counttest values (20);
insert into counttest values (30);
insert into counttest values (10);
insert into counttest values (20);
insert into counttest values (NULL);
select count(*) from counttest;
-- 6
select count(val) from counttest;
-- 5
select count(*) as count_x from counttest group by val order by count_x;
--        1 
--        1 
--        2 
--        2 
select count(val) as count_val from counttest group by val order by count_val;
--        0 
--        1 
--        2 
--        2 
drop table counttest;

commit;
