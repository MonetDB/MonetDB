CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

CREATE TABLE test (a INTEGER, b INTEGER, str VARCHAR(32));
INSERT INTO test VALUES (11, 1, 'a'), (12, 2, 'b'), (13, 3, 'c');

CREATE TABLE test2 (a INTEGER, c INTEGER, str2 VARCHAR(32));
INSERT INTO test2 VALUES (11, 1, 'a'), (12, 1, 'b'), (13, 4, 'b');

CREATE TABLE strings(v VARCHAR(32));
INSERT INTO strings VALUES ('hello'), ('world'), (NULL);

-- scalar select with correlation
SELECT i, (SELECT 42+i1.i) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 43
	-- 2, 44
	-- 3, 45

-- ORDER BY correlated subquery
SELECT i FROM integers i1 ORDER BY (SELECT 100-i1.i); -- NULL, 3, 2, 1
-- subquery returning multiple results
SELECT i, (SELECT 42+i1.i FROM integers) AS j FROM integers i1 ORDER BY i;
-- zero or one / cardinality error 
-- duckdb thinks (incorrectly)
	-- NULL, NULL
	-- 1, 43
	-- 2, 44
	-- 3, 45
-- subquery with LIMIT
SELECT i, (SELECT 42+i1.i FROM integers LIMIT 1) AS j FROM integers i1 ORDER BY i;
-- no limit (or order) in subquery error 
-- duckdb thinks
	-- NULL, NULL
	-- 1, 43
	-- 2, 44
	-- 3, 45
-- subquery with LIMIT 0
SELECT i, (SELECT 42+i1.i FROM integers LIMIT 0) AS j FROM integers i1 ORDER BY i;
-- no limit (or order) in subquery error 
-- duckdb thinks
	-- NULL, NULL
	-- 1, NULL
	-- 2, NULL
	-- 3, NULL
-- subquery with WHERE clause that is always FALSE
SELECT i, (SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, NULL
	-- 2, NULL
	-- 3, NULL
-- correlated EXISTS with WHERE clause that is always FALSE
SELECT i, EXISTS(SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;
	-- NULL, false
	-- 1, false
	-- 2, false
	-- 3, false
-- correlated ANY with WHERE clause that is always FALSE
SELECT i, i=ANY(SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;
	-- NULL, false
	-- 1, false
	-- 2, false
	-- 3, false
-- subquery with OFFSET is not supported
SELECT i, (SELECT i+i1.i FROM integers LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i; -- errror
-- correlated filter without FROM clause
SELECT i, (SELECT 42 WHERE i1.i>2) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, NULL
	-- 2, NULL
	-- 3, 42
-- correlated filter with matching entry on NULL
SELECT i, (SELECT 42 WHERE i1.i IS NULL) AS j FROM integers i1 ORDER BY i;
	-- NULL, 42		-- current bug in left outerjoin with only a select!
	-- 1, NULL
	-- 2, NULL
	-- 3, NULL
-- scalar select with correlation in projection
SELECT i, (SELECT i+i1.i FROM integers WHERE i=1) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 2
	-- 2, 3
	-- 3, 4
-- scalar select with correlation in filter
SELECT i, (SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 1
	-- 2, 2
	-- 3, 3
-- scalar select with operation in projection
SELECT i, (SELECT i+1 FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 2
	-- 2, 3
	-- 3, 4
-- correlated scalar select with constant in projection
SELECT i, (SELECT 42 FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 42
	-- 2, 42
	-- 3, 42

-- aggregate with correlation in final projection
SELECT i, (SELECT MIN(i)+i1.i FROM integers) FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 2
	-- 2, 3
	-- 3, 4
-- aggregate with correlation inside aggregation
SELECT i, (SELECT MIN(i+2*i1.i) FROM integers) FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 3
	-- 2, 5
	-- 3, 7
SELECT i, CAST(SUM(i) AS BIGINT), CAST((SELECT SUM(i)+SUM(i1.i) FROM integers) AS BIGINT) FROM integers i1 GROUP BY i ORDER BY i;
	-- NULL, NULL, NULL
	-- 1, 1, 7
	-- 2, 2, 8
	-- 3, 3, 9
SELECT i, CAST(SUM(i) AS BIGINT), CAST((SELECT SUM(i)+COUNT(i1.i) FROM integers) AS BIGINT) FROM integers i1 GROUP BY i ORDER BY i;
	-- NULL, NULL, 6
	-- 1, 1, 7
	-- 2, 2, 7
	-- 3, 3, 7
-- correlated COUNT(*)
SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;
	-- NULL, 0
	-- 1, 2
	-- 2, 1
	-- 3, 0
-- aggregate with correlation inside aggregation
SELECT i, (SELECT MIN(i+2*i1.i) FROM integers) FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 3
	-- 2, 5
	-- 3, 7
-- aggregate ONLY inside subquery
SELECT CAST((SELECT SUM(i1.i)) AS BIGINT) FROM integers i1; -- 6
-- aggregate ONLY inside subquery, with column reference outside of subquery
--SELECT FIRST(i), (SELECT SUM(i1.i)) FROM integers i1; -- missing FIRST aggregate
SELECT MIN(i), CAST((SELECT SUM(i1.i)) AS BIGINT) FROM integers i1; -- 1, 6
-- this will fail, because "i" is not an aggregate but the SUM(i1.i) turns this query into an aggregate
SELECT i, (SELECT SUM(i1.i)) FROM integers i1;
SELECT i+1, (SELECT SUM(i1.i)) FROM integers i1;
SELECT MIN(i), CAST((SELECT SUM(i1.i)) AS BIGINT) FROM integers i1; -- 1, 6
SELECT CAST((SELECT SUM(i1.i)) AS BIGINT), CAST((SELECT SUM(i1.i)) AS BIGINT) FROM integers i1; -- 6, 6
-- subquery inside aggregation
SELECT CAST(SUM(i) AS BIGINT), CAST(SUM((SELECT i FROM integers WHERE i=i1.i)) AS BIGINT) FROM integers i1; -- 6, 6
SELECT CAST(SUM(i) AS BIGINT), CAST((SELECT SUM(i) FROM integers WHERE i>SUM(i1.i)) AS BIGINT) FROM integers i1; -- 6, NULL

SELECT CAST((SELECT SUM(i) FROM integers WHERE i>SUM(i1.i)) AS BIGINT) FROM integers i1; -- NULL

SELECT i1.i FROM integers i1 INNER JOIN integers i ON SUM(i1.i) = SUM(i.i); --error, aggregations not allowed in join conditions

SELECT i1.i FROM integers i1 INNER JOIN integers i ON RANK() OVER (); --error, window functions not allowed in join conditions

-- subquery with aggregation inside aggregation should fail
SELECT SUM((SELECT SUM(i))) FROM integers; -- error
-- aggregate with correlation in filter
SELECT i, (SELECT MIN(i) FROM integers WHERE i>i1.i) FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 2
	-- 2, 3
	-- 3, NULL
-- aggregate with correlation in both filter and projection
SELECT i, (SELECT MIN(i)+i1.i FROM integers WHERE i>i1.i) FROM integers i1 ORDER BY i;
	-- NULL, NULL
	-- 1, 3
	-- 2, 5
	-- 3, NULL

SELECT (SELECT SUM(i + i1.i), 1 FROM integers) FROM integers i1; --error, the subquery must output only one column

SELECT (SELECT SUM(i1.i) FROM integers) AS k FROM integers i1 GROUP BY i ORDER BY i; --cardinality violation, scalar expression expected

SELECT i, (SELECT MIN(i) FROM integers GROUP BY i1.i) AS j FROM integers i1 ORDER BY i;
--1	1
--2	1
--3	1
--NULL	1

SELECT i, (SELECT i FROM integers GROUP BY i HAVING i=i1.i) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i1.i, CAST(SUM(i) AS BIGINT) FROM integers i1 GROUP BY i1.i HAVING SUM(i)=(SELECT MIN(i) FROM integers WHERE i<>i1.i+1) ORDER BY 1;
--1 1

SELECT i % 2 AS j, CAST(SUM(i) AS BIGINT) FROM integers i1 GROUP BY j HAVING SUM(i)=(SELECT SUM(i) FROM integers WHERE i<>j+1) ORDER BY 1;
--1 4

SELECT CAST((SELECT i+SUM(i1.i) FROM integers WHERE i=1 LIMIT 1) AS BIGINT) FROM integers i1; --error, no LIMIT on subqueries

SELECT CAST((SELECT SUM(i)+SUM(i1.i) FROM integers) AS BIGINT) FROM integers i1 ORDER BY 1;
--12

/*Wrong results
SELECT CAST((SELECT SUM(i)+SUM((CASE WHEN i IS NOT NULL THEN i*0 ELSE 0 END)+i1.i) FROM integers) AS BIGINT) FROM integers i1 ORDER BY 1;*/
--10
--14
--18
--NULL

SELECT i, CAST((SELECT i+SUM(i1.i) FROM integers WHERE i=1) AS BIGINT) FROM integers i1 GROUP BY i ORDER BY i;
--1	2
--2	3
--3	4
--NULL	NULL

SELECT CAST(SUM((SELECT i+i1.i FROM integers WHERE i=1)) AS BIGINT) FROM integers i1;
--9

SELECT i, CAST(SUM(i1.i) AS BIGINT), CAST((SELECT SUM(i1.i) FROM integers) AS BIGINT) AS k FROM integers i1 GROUP BY i ORDER BY i; --error, cardinality violation, scalar expression expected

SELECT i1.i AS j, CAST((SELECT SUM(j+i) FROM integers) AS BIGINT) AS k FROM integers i1 GROUP BY j ORDER BY j;
--1	9
--2	12
--3	15
--NULL	NULL

/*BROKEN
SELECT CAST((SELECT SUM(i1.i*i) FROM integers) AS BIGINT) FROM integers i1 ORDER BY i;*/
--6
--12
--18
--NULL

SELECT i, CAST((SELECT SUM(i1.i)) AS BIGINT) AS k, CAST((SELECT SUM(i1.i)) AS BIGINT) AS l FROM integers i1 GROUP BY i ORDER BY i;
--1	1	1
--2	2	2
--3	3	3
--NULL	NULL	NULL

SELECT i, CAST((SELECT SUM(i1.i)*SUM(i) FROM integers) AS BIGINT) AS k FROM integers i1 GROUP BY i ORDER BY i;
--1	6
--2	12
--3	18
--NULL	NULL

SELECT i AS j, CAST((SELECT j*SUM(i) FROM integers) AS BIGINT) AS k FROM integers i1 GROUP BY j ORDER BY j;
--1	6
--2	12
--3	18
--NULL	NULL

/*Wrong result, cannot find column
SELECT i AS j, CAST((SELECT i1.i*SUM(i) FROM integers) AS BIGINT) AS k FROM integers i1 GROUP BY j ORDER BY j;*/
--1	6
--2	12
--3	18
--NULL	NULL

SELECT i, CAST(SUM((SELECT SUM(i)*i1.i FROM integers)) AS BIGINT) AS k FROM integers i1 GROUP BY i ORDER BY i;
--1	6
--2	12
--3	18
--NULL	NULL

/*Wrong results, aggregation functions cannot be nested
SELECT i, SUM((SELECT SUM(i)*SUM(i1.i) FROM integers)) AS k FROM integers i1 GROUP BY i ORDER BY i; --error*/

SELECT CAST((SELECT SUM(i1.i)) AS BIGINT) FROM integers i1;
--6

SELECT FIRST(i), CAST((SELECT SUM(i1.i)) AS BIGINT) FROM integers i1; --error, no first aggregate available yet

SELECT i AS j, (SELECT MIN(i1.i) FROM integers GROUP BY i HAVING i=j) FROM integers i1 GROUP BY j ORDER BY j;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, SUM(i1.i) FROM integers i1 GROUP BY i ORDER BY (SELECT SUM(i1.i) FROM integers); --error, cardinality violation, scalar expression expected

SELECT i, SUM((SELECT SUM(i)*i1.i FROM integers LIMIT 0)) AS k FROM integers i1 GROUP BY i ORDER BY i; --error, no LIMIT on subqueries

SELECT (SELECT i+i1.i FROM integers WHERE i=1) AS k, CAST(SUM(i) AS BIGINT) AS j FROM integers i1 GROUP BY k ORDER BY 1;
--2	1
--3	2
--4	3
--NULL	NULL

SELECT CAST(SUM(i) AS BIGINT) FROM integers i1 WHERE i>(SELECT (i+i1.i)/2 FROM integers WHERE i=1);
--5

SELECT CAST(SUM(i) AS BIGINT) FROM integers i1 WHERE i>(SELECT (SUM(i)+i1.i)/2 FROM integers WHERE i=1);
--5

SELECT i, (SELECT MIN(i) FROM integers WHERE i=i1.i) >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;
--1	False
--2	False
--3	True
--NULL	NULL

SELECT i, (SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;
--1	True
--2	False
--3	False
--NULL	NULL

SELECT i, NOT((SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS NOT NULL)) FROM integers i1 ORDER BY i;
--1	False
--2	True
--3	True
--NULL	NULL

/* Wrong results
SELECT i, (SELECT i FROM integers i2 WHERE i=(SELECT SUM(i) FROM integers i2 WHERE i2.i>i1.i)) FROM integers i1 ORDER BY 1;*/
--1	NULL
--2	3
--3	NULL
--NULL	NULL

SELECT i, CAST((SELECT SUM(i) IS NULL FROM integers i2 WHERE i2.i>i1.i) AS BIGINT) FROM integers i1 ORDER BY i;
--1	False
--2	False
--3	True
--NULL	True

SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;
--1	2
--2	1
--3	0
--NULL	0

SELECT i, (SELECT COUNT(i) FROM integers i2 WHERE i2.i>i1.i OR i2.i IS NULL) FROM integers i1 ORDER BY i;
--1	2
--2	1
--3	0
--NULL	0

SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i OR i2.i IS NULL) FROM integers i1 ORDER BY i;
--1	3
--2	2
--3	1
--NULL	1

SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i OR (i1.i IS NULL AND i2.i IS NULL)) FROM integers i1 ORDER BY i;
--1	2
--2	1
--3	0
--NULL	1

/*Wrong results
SELECT i FROM integers i1 WHERE (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i)=0 ORDER BY i;*/
--3
--NULL

/*Wrong results
SELECT i, (SELECT i FROM integers i2 WHERE i-2=(SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i)) FROM integers i1 ORDER BY 1;*/
--1	NULL
--2	3
--3	2
--NULL	2

/*Wrong results
SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i GROUP BY i1.i) FROM integers i1 ORDER BY i;*/
--1	2
--2	1
--3	NULL
--NULL	NULL

SELECT i, (SELECT CASE WHEN (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i)=0 THEN 1 ELSE 0 END) FROM integers i1 ORDER BY i;
--1	0
--2	0
--3	1
--NULL	1

SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;
--1	2
--2	1
--3	0
--NULL	0

SELECT a, CAST(SUM(a) AS BIGINT), CAST((SELECT SUM(a)+SUM(t1.b) FROM test) AS BIGINT) FROM test t1 GROUP BY a ORDER BY a;
--11	11	37
--12	12	38
--13	13	39

SELECT CAST((SELECT test.a+test.b+SUM(test2.a) FROM test2 WHERE "str"=str2) AS BIGINT) FROM test ORDER BY 1;
--NULL
--23
--39

SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE test.a=test2.a AND test.b<>test2.c);
--12	2	b
--13	3	c

SELECT a, a>=ANY(SELECT test2.a+c-b FROM test2 WHERE c>=b AND "str"=str2) FROM test ORDER BY 1;
--11	true
--12	false
--13	false

SELECT "str", "str"=ANY(SELECT str2 FROM test2) FROM test;
--a	true
--b	true
--c	false

SELECT "str", "str"=ANY(SELECT str2 FROM test2 WHERE test.a<>test2.a) FROM test;
--a	false
--b	true
--c	false

SELECT i, (SELECT s1.i FROM (SELECT * FROM integers WHERE i=i1.i) s1) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

/*Wrong results
SELECT i, (SELECT s1.i FROM (SELECT i FROM integers WHERE i=i1.i) s1 INNER JOIN (SELECT i FROM integers WHERE i=4-i1.i) s2 ON s1.i>s2.i) AS j FROM integers i1 ORDER BY i;*/
--1	NULL
--2	NULL
--3	3
--NULL	NULL

/*Mitosis error I think (it runs fine without it)
SELECT i, (SELECT s1.i FROM integers s1, integers s2 WHERE s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;*/
--1	3
--2	2
--3	1

/*Mitosis error I think (it runs fine without it)
SELECT i, (SELECT s1.i FROM integers s1 INNER JOIN integers s2 ON s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;*/
--1	3
--2	2
--3	1

/*Extra projection
SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;*/
--1 2

SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=s2.i) ORDER BY s1.i;
--1	1
--2	2
--3	3

/*Extra projection
SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=i FROM integers WHERE s2.i=i) ORDER BY s1.i;*/
--1	1
--2	2
--3	3

/*Wrong results
SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;*/
--1	2
--2	NULL
--3	NULL
--NULL	NULL

SELECT i, CAST((SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i OR s1.i=i1.i-1) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	6
--2	9
--3	12
--NULL	6

SELECT i, CAST((SELECT SUM(s1.i) FROM integers s1 FULL OUTER JOIN integers s2 ON s1.i=s2.i OR s1.i=i1.i-1) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	6
--2	9
--3	12
--NULL	6

--SELECT i, (SELECT row_number() OVER (ORDER BY i)) FROM integers i1 ORDER BY i; --Should we support correlated expressions inside PARTITION BY and ORDER BY on Window functions?

/* Extra projection
SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;*/
--1	1
--2	2
--3	3
--NULL	NULL

/* Extra projection
SELECT i, (SELECT i FROM integers WHERE i IS NOT NULL EXCEPT SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 WHERE i IS NOT NULL ORDER BY i;*/
--1	1
--2	2
--3	3

/* Extra projection
SELECT i, (SELECT i FROM integers WHERE i=i1.i INTERSECT SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;*/
--1	1
--2	2
--3	3
--NULL	NULL

/* Extra projection
SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION SELECT i FROM integers WHERE i<>i1.i EXCEPT SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 ORDER BY i;*/
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT (SELECT SUM(i) FROM integers)+42+i1.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	49
--2	50
--3	51
--NULL	NULL

SELECT i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1 ORDER BY i;
--1	1
--2	1
--3	1
--NULL	NULL

/*Wrong results
SELECT i1.i, (SELECT rank() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1, integers i2 ORDER BY i1.i;*/
--NULL,	NULL
--NULL,	NULL
--NULL,	NULL
--NULL,	NULL
--1,	1
--1,	1
--1,	1
--1,	1
--2,	1
--2,	1
--2,	1
--2,	1
--3,	1
--3,	1
--3,	1
--3,	1

/*Wrong results
SELECT i1.i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1, integers i2 ORDER BY i1.i;*/
--1	1
--1	1
--1	1
--1	1
--2	1
--2	1
--2	1
--2	1
--3	1
--3	1
--3	1
--3	1
--NULL	NULL
--NULL	NULL
--NULL	NULL
--NULL	NULL

/*MAL error
SELECT i, CAST((SELECT SUM(i) OVER (ORDER BY i) FROM integers WHERE i1.i=i) AS BIGINT) FROM integers i1 ORDER BY i;*/
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, (SELECT SUM(s1.i) OVER (ORDER BY s1.i) FROM integers s1, integers s2 WHERE i1.i=s1.i LIMIT 1) FROM integers i1 ORDER BY i; --error

SELECT i, CAST((SELECT (SELECT 42+i1.i)+42+i1.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	86
--2	88
--3	90
--NULL	NULL

SELECT i, CAST((SELECT (SELECT (SELECT (SELECT 42+i1.i)++i1.i)+42+i1.i)+42+i1.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	130
--2	134
--3	138
--NULL	NULL

SELECT i, CAST((SELECT (SELECT i1.i+SUM(i2.i)) FROM integers i2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	7
--2	8
--3	9
--NULL	NULL

SELECT i, CAST((SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i)))) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	10
--3	15
--NULL	NULL

SELECT i, CAST((SELECT SUM(i)+(SELECT 42+i1.i) FROM integers) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	49
--2	50
--3	51
--NULL	NULL

/* BROKEN, cannot find column
SELECT i, (SELECT ((SELECT ((SELECT ((SELECT SUM(i)+SUM(i4.i)+SUM(i3.i)+SUM(i2.i)+SUM(i1.i) FROM integers i5)) FROM integers i4)) FROM integers i3)) FROM integers i2) AS j FROM integers i1 GROUP BY i ORDER BY i;*/
--1	25
--2	26
--3	27
--NULL	NULL

SELECT i, CAST((SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i+i2.i) FROM integers i2 WHERE i2.i=i1.i))) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	6
--2	12
--3	18
--NULL	NULL

--BROKEN
--SELECT i, (SELECT SUM(s1.i) FROM integers s1 INNER JOIN integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;
--1	6
--2	6
--3	6
--NULL	NULL

--BROKEN
--SELECT i, SUM(i), (SELECT (SELECT SUM(i)+SUM(i1.i)+SUM(i2.i) FROM integers) FROM integers i2) FROM integers i1 GROUP BY i ORDER BY i;
--1	1 13
--2	2 14
--3	3 15
--NULL	NULL	NULL

SELECT i, CAST((SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	5
--3	5
--NULL	5

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

--BROKEN
--SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;
--1	6
--2	6
--3	6
--NULL	6

SELECT i, CAST((SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM
integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	10
--2	10
--3	10
--NULL	10

SELECT i, CAST((SELECT SUM(s1.i) FROM (SELECT i FROM integers WHERE i=i1.i) s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(s1.i) FROM (SELECT i FROM integers WHERE i<>i1.i) s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	4
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE i=i1.i) s2 ON s1.i=s2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE i<>i1.i) s2 ON s1.i=s2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	4
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE i=i1.i) s2 ON s1.i=s2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE i<>i1.i) s2 ON s1.i=s2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	4
--3	3
--NULL	NULL

--BROKEN
--SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE CASE WHEN (i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i) ss2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	6
--2	6
--3	6
--NULL	6

/*Wrong result
SELECT i, (SELECT i=ANY(SELECT i FROM integers WHERE i=s1.i) FROM integers s1 WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;*/
--1	True
--2	True
--3	True
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i OR i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	6
--2	6
--3	6
--NULL	6

--BROKEN
--SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE CASE WHEN (i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM integers WHERE i=s1.i)) ss2) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	1
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	5
--3	5
--NULL	5

SELECT i, CAST((SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 
	WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	5
--2	5
--3	5
--NULL	5

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN 
	(SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	NULL
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN
	(SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	NULL
--2	2
--3	3
--NULL	NULL

SELECT i, CAST((SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN
	(SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;
--1	NULL
--2	7
--3	8
--NULL	NULL

/*Wrong result
SELECT i, CAST((SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN
	(SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS BIGINT) AS j FROM integers i1 ORDER BY i;*/
--1	NULL
--2	4
--3	6
--NULL	NULL

/*Wrong result
SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN
	(SELECT i FROM integers s1 WHERE i<>i1.i OR i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;*/
--1	NULL
--2	4
--3	6
--NULL	NULL

SELECT NULL IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v;
--NULL
--NULL
--False

SELECT 3 IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v; --error, cannot cast 3 into string

SELECT 'hello' IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v;
--True
--False
--False

SELECT 'hello' IN (SELECT * FROM strings WHERE v=s1.v or v IS NULL) FROM strings s1 ORDER BY v;
--True
--NULL
--NULL

SELECT 'bla' IN (SELECT * FROM strings WHERE v=s1.v or v IS NULL) FROM strings s1 ORDER BY v;
--NULL
--NULL
--NULL

/*BROKEN
SELECT * FROM strings WHERE EXISTS(SELECT NULL, v) ORDER BY v;*/
--'hello'
--'world'
--NULL

SELECT * FROM strings s1 WHERE EXISTS(SELECT v FROM strings WHERE v=s1.v OR v IS NULL) ORDER BY v;
--hello
--world
--NULL

SELECT * FROM strings s1 WHERE EXISTS(SELECT v FROM strings WHERE v=s1.v) ORDER BY v;
--hello
--world

SELECT (SELECT v FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v;
--hello
--world
--NULL

SELECT (SELECT v FROM strings WHERE v=s1.v OR (v='hello' AND s1.v IS NULL)) FROM strings s1 ORDER BY v;
--hello
--hello
--world

DROP TABLE integers;
DROP TABLE test;
DROP TABLE test2;
DROP TABLE strings;
