create table coalescetest (id int,
                   age int,
                   name varchar(20));
insert into coalescetest values (1,20,'a');
insert into coalescetest(id,age) values (2,26);
insert into coalescetest(id,name) values (3,'c');
insert into coalescetest(id) values (4);

select avg(coalesce(age,38)) from coalescetest;

--detect the NULL value for column name
select id, coalesce(name,'user unknown') from coalescetest;

--detect the NULL value for column age
select id, coalesce(age,'age unknown') from coalescetest;
select id, coalesce(age,-1) from coalescetest;

--detect the NULL value for column age and name
select id, coalesce(name, age, 'unknown') from coalescetest;

SELECT COALESCE(NULL,'x');

---Coalesce in the where clause:

--select only the id where the name is unknown
select id, name, age from coalescetest where coalesce(name,'unknown') LIKE 'unknown';

--select the name where the id > age
select id, name, age from coalescetest where coalesce(id, 0) < coalesce(age, 1) and coalesce(name,'unknown') LIKE 'unknown';

drop table coalescetest;
