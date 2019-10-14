CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

/*Wrong results
SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>2) FROM integers i1 ORDER BY i;*/
--1	False
--2	False
--3	True
--NULL	False

SELECT i, EXISTS(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;
--1	True
--2	True
--3	True
--NULL	False

SELECT i FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;
--1
--2
--3

/*Wrong results
SELECT EXISTS(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;*/
--True

SELECT i, CAST(SUM(i) AS BIGINT) FROM integers i1 GROUP BY i HAVING EXISTS(SELECT i FROM integers WHERE i>MIN(i1.i)) ORDER BY i;
--1 1
--2 2

/*Wrong results
SELECT EXISTS(SELECT i+MIN(i1.i) FROM integers WHERE i=3) FROM integers i1;*/
--True

/*Wrong results
SELECT EXISTS(SELECT i+MIN(i1.i) FROM integers WHERE i=5) FROM integers i1;*/
--False

SELECT EXISTS(SELECT i FROM integers WHERE i=i1.i) AS g, COUNT(*) FROM integers i1 GROUP BY g ORDER BY g;
--False	1
--True	3

SELECT CAST(SUM(CASE WHEN EXISTS(SELECT i FROM integers WHERE i=i1.i) THEN 1 ELSE 0 END) AS BIGINT) FROM integers i1;

DROP TABLE integers;
