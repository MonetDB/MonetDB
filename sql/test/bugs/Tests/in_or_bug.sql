CREATE TABLE "sys"."t1" ("c1" varchar(3),"c2" varchar(3), "c3" varchar(3));
insert into t1 values (1,2,3);
insert into t1 values (3,2,1);
select count(*) from t1 where c1='1' and (c2='3' or c3='3');
select count(*) from t1 where c1='1' and (c2 in ('3') or c3 in ('3'));
drop table t1;
