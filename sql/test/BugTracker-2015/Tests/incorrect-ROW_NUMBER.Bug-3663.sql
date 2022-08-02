CREATE TABLE t1 (id VARCHAR(48), col1 VARCHAR(32), col2 VARCHAR(8), excepted BOOLEAN);
INSERT INTO t1 (id, col1, col2, excepted) VALUES ('12', 'col1.A.99.code', '.03', 'false'), ('12', 'col1.A.99.code', '.02', 'false');

CREATE TABLE t2 (id  VARCHAR(48), col3 VARCHAR(32), col2 VARCHAR(8), row int);
INSERT INTO t2 (id, col3, col2, row) VALUES ('12',null,null,null);
UPDATE t2 SET (col3, col2, row) =
(SELECT col1, col2, row
FROM (
	SELECT id, col1, col2,
		   ROW_NUMBER() OVER (PARTITION BY id ORDER BY (col1 LIKE '%.%.99.%') ASC,
			                  col1 ASC, col2 ASC) AS row
	FROM t1
	WHERE excepted = false
  	  AND col1 LIKE '%.A.%'
)  AS t3
WHERE t3.row = 1
AND t2.id= t3.id
);
SELECT * FROM t2;

DROP TABLE t2;
CREATE TABLE t2 (id  VARCHAR(48), col3 VARCHAR(32), col2 VARCHAR(8), row int);
INSERT INTO t2 (id, col3, col2, row) VALUES ('12',null,null,null);
CREATE TABLE t3 AS (
	SELECT id, col1, col2,
	       ROW_NUMBER() OVER (PARTITION BY id ORDER BY (col1 LIKE '%.%.99.%') ASC,
			                  col1 ASC,col2 ASC) AS row
	FROM t1
	WHERE excepted = false AND col1 LIKE '%.A.%'
) WITH DATA;
UPDATE t2 SET (col3, col2, row) = (SELECT col1, col2, row FROM t3 WHERE t2.id= t3.id AND t3.row = 1);
SELECT * FROM t2;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
