create table test (id int,
                   age int,
                   name varchar(20));
insert into test values (1,20,'a');
insert into test(id,age) values (2,26);
insert into test(id,name) values (3,'c');

select avg(coalesce(age,38)) from test;
select id, coalesce(name,'unknown') from test;
drop table test;

SELECT COALESCE(NULL,'x');
