
CREATE TABLE foo (a INT, b INT);
INSERT INTO foo VALUES (10, 3), (2, 5), (0, 8), (7, 10), (1, 1), (9, 12), (4, 1), (3, 9);

SELECT *
FROM (SELECT CAST(SUM(a) AS FLOAT) / SUM(b) AS result
	      FROM foo
	      GROUP BY a) as t1
ORDER BY (1=1), result DESC
LIMIT 5;

drop table foo;
