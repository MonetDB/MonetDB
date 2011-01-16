 CREATE TABLE union_test (
id INT,
val1 VARCHAR(10),
val2 VARCHAR(10)
);
INSERT INTO union_test VALUES(1, 'abcdef','123456');
SELECT 'abc' AS c1, '123' AS c2
UNION ALL
SELECT val1 AS c1, val2 AS c2 FROM union_test;
