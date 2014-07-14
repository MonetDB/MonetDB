create table test (id int, s varchar(255));
insert into test values(1, 'a');
insert into test values(2, 'b');
insert into test values(3, 'c');
select a.id, b.id from test a,test b where a.id < b.id;

create view test2 as select * from test;
select a.id, b.id from test2 a,test2 b where a.id < b.id;

drop view test2;
drop table test;
