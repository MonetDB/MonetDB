start transaction;

create table table3389 (k int,b int);
insert into table3389 values (1,2);
insert into table3389 values (2,2);
insert into table3389 values (3,3);
insert into table3389 values (4,65);
insert into table3389 values (5,21);
insert into table3389 values (6,null);
insert into table3389 values (7,null);
insert into table3389 values (8,null);
insert into table3389 values (9,null);

select median(b) from table3389 group by k;

rollback;
