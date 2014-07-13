create table t1 ( a int, b int ) ;
create table t2 ( a int, b int ) ;
insert into t1 values ( 1, 1 ), ( 2, 2 ) ;
insert into t2 values ( 1, 1 ) ;

--the result of
select * from t1 where exists( select * from t2 where t2.a = t1.a ) or t1.b
> 0 ;

--is the same as the result of
select * from t1 where exists( select * from t2 where t2.a = t1.a ) and
t1.b > 0 ;

--but the 'OR' should return two records.

drop table t1;
drop table t2;
