create table test (id int,
                   age int,
                   name varchar(20));
insert into test values (1,20,'a');
insert into test(id,age) values (2,26);
insert into test(id,name) values (3,'c');
insert into test(id) values (3);

select avg(coalesce(age,38)) from test;

--detect the NULL value for column name
select id, coalesce(name,'unknown') from test;

--detect the NULL value for column age
select id, coalesce(age,'unknown') from test;

--detect the NULL value for column age and name
select id, coalesce(name, age, 'unknown') from test;


delete from test;
insert into test(id) values (3);

select id, coalesce(name, age, 'unknown') from test;

drop table test;

SELECT COALESCE(NULL,'x');

---Coalesce in the where clause:
create table test (id int,
                   age int,
                   name varchar(20));
insert into test values (1,20,'a');
insert into test(id,age) values (2,26);
insert into test(id,name) values (3,'c');
insert into test(id) values (3);

--select only the id where the name is unknown
select id, name, age from test where coalesce(name,'unknown') LIKE 'unknown';

--select the name where the id > age
select id, name, age from test where coalesce(id, 0) < coalesce(age, 1) and coalesce(name,'unknown') LIKE 'unknown';

drop table test;
