create table bug2739 (val int);
insert into bug2739 values (1), (2);
select avg(val - (select avg(val) from bug2739)) from bug2739;
drop table bug2739;
