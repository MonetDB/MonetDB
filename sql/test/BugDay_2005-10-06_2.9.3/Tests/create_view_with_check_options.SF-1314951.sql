start transaction;
CREATE TABLE t1314951 (v int);
INSERT INTO t1314951 VALUES (1),(2),(3),(4);
create view m as select * from t1314951 with check option;
select * from m;
