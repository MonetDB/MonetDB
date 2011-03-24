create table t2790 (d double);
insert into t2790 values (2),(3);
select avg(d) as avg_d, avg(d*d) as avg_d2, avg(d)*avg(d) as avg2_d_mult from t2790;
create view tv2790 as select avg(d) as avg_d, avg(d*d) as avg_d2, avg(d)*avg(d) as avg2_d_mult from t2790;

select * from tv2790;

drop view tv2790;
drop table t2790;
