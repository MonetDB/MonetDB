create table t1 (aa int, bb int, cc int);
insert into t1 values (1,1,1);
select t2.dd, t2.ee, t2.ff from t1 as t2(dd,ee,ff);
select t2.cc from t1 as t2(dd,ee,cc);
select t2.ee from t1 as t2(dd,ee,ff);
select t2.cc from t1 as t2(dd); --error
select t2.ee from t1 as t2(dd,ee); --error
select t2.aa from t1 as t2(dd,dd,cc); --error
select t2.dd from t1 as t2(dd,ee,ff,gg); --error
select t3.output from generate_series(1, 2) as t3(output);

select * from (values(1,2)) as a(a); --error

create table t2 as (select count(*) from t1); --error, labels not allowed in column names
create table t2 as (select count(*) as "mylabel" from t1); --allowed
select count(*) from t2;

drop table t1;
drop table t2;
