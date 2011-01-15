create table basket10(x int, y int);
insert into basket10 values(1,1),(2,3);
select * from [select * from basket10] as t;
-- next one caused a seg fault
select * from [select * from basket10] as t;
drop table basket10;
