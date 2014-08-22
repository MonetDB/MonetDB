CREATE TABLE test_num_data (id integer, val numeric(18,10));
INSERT INTO test_num_data VALUES (1, '-0.0');
INSERT INTO test_num_data VALUES (2, '-34338492.215397047');
SELECT * FROM test_num_data;
SELECT t1.id, t2.id, t1.val * t2.val FROM test_num_data t1, test_num_data t2;
SELECT t1.id, t2.id, round(t1.val * t2.val, 30) FROM test_num_data t1, test_num_data t2;
drop table test_num_data;
