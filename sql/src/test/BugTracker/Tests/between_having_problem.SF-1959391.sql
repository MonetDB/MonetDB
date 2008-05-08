create table t3 (id float);
select id from t3 where (id between 0 and 62) having (id = id+.1 or id = id
- .1);

drop table t3;
