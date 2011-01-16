create table test (id serial, val int);
insert into test (val) values (1),(1),(1);
select * from test;
drop table test;
create table test2 (t int unique);
insert into test2 values (1),(1),(1);
select * from test2;
drop table test2;
