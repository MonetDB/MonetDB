start transaction;
create table test (age smallint);
insert into test values ('');
rollback;
start transaction;
create table test (age smallint);
insert into test values ('');
rollback;
