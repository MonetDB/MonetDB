create table t1684798a (name varchar(1024), address varchar(1024), age int);
create table t1684798b (street varchar(1024), id int);

insert into t1684798a values('romulo', 'amsterdam', 23);
insert into t1684798b values('amsterdam', 1);

select name from t1684798a, t1684798b where t1684798a.address LIKE 'amsterdam';
select name from t1684798a, t1684798b where t1684798a.address LIKE t1684798b.street;

drop table t1684798a;
drop table t1684798b;
