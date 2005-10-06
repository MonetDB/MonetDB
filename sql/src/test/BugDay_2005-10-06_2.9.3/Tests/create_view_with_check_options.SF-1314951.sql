CREATE TABLE test (v int);
INSERT INTO test VALUES (1),(2),(3),(4);
create view m as select * from test with check option;
select * from m;
