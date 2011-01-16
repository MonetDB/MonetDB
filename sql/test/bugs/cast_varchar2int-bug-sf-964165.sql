create table t2 (x varchar(10));
insert into t2 values ('1');
insert into t2 values ('2');

select cast(x as integer) from t2;
select cast('1' as integer);

drop table t2;
