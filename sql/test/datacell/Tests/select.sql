create table basket12(x int, y int);
insert into basket12 values(1,1),(2,3);
select * from [select * from basket12 where y >1] as t;
drop table basket12;
