create table t (t1 timestamp, t2 timestamp(0), t3 timestamp(3));
insert into t values (now(), now(), now());
select * from t;
