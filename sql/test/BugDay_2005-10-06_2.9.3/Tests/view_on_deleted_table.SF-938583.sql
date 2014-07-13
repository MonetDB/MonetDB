create table t_vdt (i int);
create view v_vdt as select * from t_vdt;
drop table t_vdt;
select * from v_vdt;
