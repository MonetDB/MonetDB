CREATE TABLE test (v int);
INSERT INTO test VALUES (1),(2),(3),(4);
create view m as select count(*) from test;
select * from m;
