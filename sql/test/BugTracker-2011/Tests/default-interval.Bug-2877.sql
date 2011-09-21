start transaction;
create table table2877 (
       i int,
       c1 interval hour to second default interval '1:00:00' hour to second,
       c2 interval hour to second default 3600
);
insert into table2877 (i) values (1);
select * from table2877;
rollback;
