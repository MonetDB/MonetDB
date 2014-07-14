start transaction;

create table table2894 (i int, d double);
insert into table2894 values (1,1.0),(2,2.0),(3,3.0),(4,4.0);
select i,1/(i-1) from table2894 where i > 1;
select i,1/(i-1.0) from table2894 where i > 1;
select i,1/(i-1) from table2894 where i > 1;

rollback;
