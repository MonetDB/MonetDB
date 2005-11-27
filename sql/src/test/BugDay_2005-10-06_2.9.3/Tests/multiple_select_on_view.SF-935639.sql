start TRANSACTION;
create table a (b integer);
insert into a values(1);
insert into a values(2);
create view c as select * from a;
select count(*) from c;
select count(*) from c;
