create table test (a integer, b integer);
insert into test (a) values (select 2);
insert into test (a,b) values (select 2, 2);

drop table test;
