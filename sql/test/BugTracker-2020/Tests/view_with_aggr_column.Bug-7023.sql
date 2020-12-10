start transaction;
create table tst (k integer  not null, name char(20)  not null);
-- this works
select min(k) from tst group by name;
-- but putting it in a VIEW doesn't
create view v1 as select max(k) from tst group by name;
select * from v1;
rollback;
