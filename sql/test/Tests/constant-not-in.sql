CREATE TABLE mytable (i INTEGER);
INSERT INTO mytable VALUES (1);

-- all these queries should return 'b'

SELECT 'b' AS c FROM mytable;

SELECT *
FROM (SELECT 'b' AS c FROM mytable) t;

SELECT *
FROM (SELECT 'b' AS c FROM mytable) t
WHERE c IN ('b');

SELECT *
FROM (SELECT 'b' AS c FROM mytable) t
WHERE c NOT IN ('a');

DROP TABLE mytable;
