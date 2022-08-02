create table t1 ( c1 int, c2 int );
select count(c1) as c2 from (select count(*) as c1, c2 from t1 group by c2) as t2 group by c2;

select count(c1_) as c2 from (select count(*) as c1, c2 from t1 group by c2) as t2 group by c2;
drop table t1;
