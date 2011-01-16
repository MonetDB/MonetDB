create table t31959391 (id float);
select id from t31959391 where (id between 0 and 62) having (id = id+.1 or id = id
- .1);

drop table t31959391;
