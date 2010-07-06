create table a (x int, y int);
insert into a values (1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(1,2),(2,1),(2,3);
select * from (select 1 as id , count(*) as cnt from (select distinct x,y from a) as b) as b2 join (select 1 as id , count(*) as cnt2 from a) as a2 on b2.id = a2.id;
select * from (select 1 as id , count(*) as cnt from (select distinct x,y from a) as b) as b2, (select 1 as id , count(*) as cnt2 from a) as a2 where b2.id = a2.id;
drop table a;

