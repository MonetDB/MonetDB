create table t_add_bool ("v1" int,"v2" int, "v3" int);
insert into t_add_bool ("v1","v2","v3") values (1,1,1);
insert into t_add_bool ("v1","v2","v3") values (1,2,1);
insert into t_add_bool("v1","v2","v3") values (1,2,3);
insert into t_add_bool ("v1","v2","v3") values (4,4,4);
select * from t_add_bool where (("v1" > 1)+("v2" > 1)+("v3" > 1)) > 1;
drop table t_add_bool;
