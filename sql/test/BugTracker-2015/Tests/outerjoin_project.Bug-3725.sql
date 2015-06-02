start transaction;
create table a (a integer);
create table b (a integer);
insert into a values (1);
select * from a left join (select a, 20 from b) as x using (a);
rollback;
