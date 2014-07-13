create table boolean_test (b boolean not null, i int not null);
insert into boolean_test (b, i) values (true, 1), (true, 2), (true, 3), (true, 4);
select * from boolean_test;
select * from boolean_test where b and (i < 3);
select * from boolean_test where b = true and (i < 3);
drop table boolean_test;
