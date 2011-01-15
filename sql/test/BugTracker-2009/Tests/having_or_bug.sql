create table htest(links integer, rechts integer);
insert into htest values (1, 4);
select count(*) from htest group by links having count(links) > 0 or
count(links) < 3;

drop table htest;
