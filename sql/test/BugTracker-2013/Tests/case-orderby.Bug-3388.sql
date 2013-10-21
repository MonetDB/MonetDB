start transaction;

create table table3388 (k int,b int);
insert into table3388 values (1,2);
insert into table3388 values (2,2);
insert into table3388 values (3,3);
insert into table3388 values (4,65);
insert into table3388 values (5,21);
insert into table3388 values (6,null);
insert into table3388 values (7,null);
insert into table3388 values (8,null);
insert into table3388 values (9,null);

select sum(b) from table3388 group by k order by case when sum(b) is null then 1 else 0 end,sum(b);

rollback;
