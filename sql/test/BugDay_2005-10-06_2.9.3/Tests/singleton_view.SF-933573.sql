CREATE TABLE t_sv (v int);
INSERT INTO t_sv VALUES (1),(2),(3),(4);
create view v_sv as select count(*) from t_sv;
select * from v_sv;
