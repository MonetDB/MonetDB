create table weekofyear_test (d date);
insert into weekofyear_test values ('1986-JAN-01');
insert into weekofyear_test values ('1987-JAN-01');
insert into weekofyear_test values ('1988-JAN-01');
insert into weekofyear_test values ('1989-JAN-01');
insert into weekofyear_test values ('1990-JAN-01');

select d, weekofyear(d), dayofweek(d), dayofmonth(d) from weekofyear_test;
drop table weekofyear_test;
