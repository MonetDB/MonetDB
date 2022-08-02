create table t_2599 (a int, b int);
-- work:
prepare select * from t_2599 where a>1+?;
prepare select * from t_2599 where a>2*?+1;
-- crash:
prepare select * from t_2599 where a>?+1;
prepare select * from t_2599 where a>1+?*2;
-- cleanup:
drop table t_2599;
