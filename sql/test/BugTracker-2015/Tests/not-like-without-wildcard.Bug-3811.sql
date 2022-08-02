start transaction;
create table test (id int, value text);
insert into test values (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
select * from test;
select * from test where value not like 'Bob';
select * from test where value not ilike 'Bob';
rollback;
