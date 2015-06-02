start transaction;
create table test (a inet);
insert into test (a) values ('10.0.0.1');
select * from test where a << inet '10.0.0.0/8';

create schema test;
set schema test;
select * from sys.test where a << inet '10.0.0.0/8';

rollback;


