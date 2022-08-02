create table t_2906 (a_2906 int);
insert into t_2906 values (1);
select * from t_2906;
alter table t_2906 set read only;
insert into t_2906 values (2);
select * from t_2906;
drop table t_2906;
