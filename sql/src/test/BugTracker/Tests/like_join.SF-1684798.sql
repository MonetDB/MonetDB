create table t1 (name varchar(1024), address varchar(1024), age int);
create table t2 (street varchar(1024), id int);

insert into t1 values('romulo', 'amsterdam', 23);
insert into t2 values('amsterdam', 1);

select name from t1, t2 where t1.address LIKE 'amsterdam';
select name from t1, t2 where t1.address LIKE t2.street;

drop table t1;
drop table t2;
