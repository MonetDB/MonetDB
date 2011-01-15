create table a (b int, c int);
insert into a (b, c) values (10, 100);
insert into a (b, c) values (10, 200);
select b, count(distinct c) from a group by b having count(distinct c) > 1;
drop table a;
