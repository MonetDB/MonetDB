create table test (id int, s varchar(255));
insert into test values(1, 'a');
insert into test values(2, 'b');
insert into test values(3, 'c');
commit;
select a.id, b.id from test a,test b where a.id < b.id;

create view test2 as select * from test;
commit;
select a.id, b.id from test2 a,test2 b where a.id < b.id;
