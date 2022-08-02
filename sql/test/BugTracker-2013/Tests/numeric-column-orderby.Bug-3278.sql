start transaction;
create table testorder (a string, b string);
insert into testorder values ('a', 'z'), ('b', 'y'), ('c', 'x');
select * from testorder;
select * from testorder order by 1;
select * from testorder order by 2;
rollback;
