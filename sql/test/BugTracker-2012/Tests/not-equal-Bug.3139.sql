create table t1_bug3139 (id int,dp int);
create table t2_bug3139 (id int, s int);
insert into t1_bug3139 values (1,1),(2,2),(3,2),(4,2),(5,3),(6,3),(7,1);
insert into t2_bug3139 values (1,1),(1,2),(1,3);
select t1_bug3139.id, t1_bug3139.dp, t3.cnt
from t1_bug3139,
     (select count(*) as cnt from t2_bug3139) t3
where t1_bug3139.id > 1 and t1_bug3139.dp <> t3.cnt;

declare t2_bug3139_cnt bigint;
set t2_bug3139_cnt = (select count(*) from t2_bug3139);
select t1_bug3139.id, t1_bug3139.dp, t2_bug3139_cnt
from t1_bug3139
where t1_bug3139.id > 1 and t1_bug3139.dp <> t2_bug3139_cnt;

select t1_bug3139.id, t1_bug3139.dp
from t1_bug3139
where t1_bug3139.id > 1 and t1_bug3139.dp <> (select count(*) from t2_bug3139);
