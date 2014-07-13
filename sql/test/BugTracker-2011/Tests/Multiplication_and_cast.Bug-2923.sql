create table test234(t decimal(3,2));
insert into test234 (t) values (-1.21);
select cast((t * 100) as integer) from test234;
drop table test234;
