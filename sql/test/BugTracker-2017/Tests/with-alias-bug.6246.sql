CREATE TABLE test1 (A INT NOT NULL, B INT NOT NULL DEFAULT -1);
INSERT INTO test1(A) VALUES (1), (2), (3);

CREATE TABLE test2 (X INT NOT NULL);
INSERT INTO test2 VALUES (10), (20);


UPDATE test1
SET B = test3.X *10
FROM (
  WITH test3 AS (SELECT * FROM test2)
  SELECT X FROM test3
) AS t2
WHERE 10 * A = test3.X;

SELECT * FROM test1;
/*
+------+------+
| a    | b    |
+======+======+
|    1 |  100 |
|    2 |  200 |
|    3 |   -1 |
+------+------+
*/

UPDATE test1
SET B = t2.X
FROM (
  WITH test3 AS (SELECT * FROM test2)
  SELECT X FROM test3
) AS t2
WHERE 10 * A = t2.X;
-- SELECT: no such column 't2.x'

-- cleanup
DROP TABLE test1;
DROP TABLE test2;
