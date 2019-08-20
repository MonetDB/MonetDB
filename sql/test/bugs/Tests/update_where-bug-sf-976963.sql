create table test_update (id varchar(1), x int);
insert into test_update values ('a', 1);
insert into test_update values ('b', 2);
select * from test_update order by id;
update test_update set x=x+1 where x >= 2;
select * from test_update order by id;
