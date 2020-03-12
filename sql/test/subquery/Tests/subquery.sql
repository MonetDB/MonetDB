CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
SELECT * FROM integers WHERE i=(SELECT 1); -- 1
SELECT * FROM integers WHERE i=(SELECT SUM(1)); -- 1
SELECT * FROM integers WHERE i=(SELECT MIN(i) FROM integers); --1
SELECT * FROM integers WHERE i=(SELECT MAX(i) FROM integers); --3
SELECT *, (SELECT MAX(i) FROM integers) FROM integers ORDER BY i;
	-- NULL, 3
	-- 1, 3
	-- 2, 3
	-- 3, 3
SELECT (SELECT 42) AS k, MAX(i) FROM integers GROUP BY k; -- 42, 3

SELECT i, MAX((SELECT 42)) FROM integers GROUP BY i ORDER BY i;
	-- NULL, 42
	-- 1, 42
	-- 2, 42
	-- 3, 42
SELECT (SELECT * FROM integers WHERE i>10) FROM integers;
	-- NULL
	-- NULL
	-- NULL
	-- NULL
-- Errors as we do not expect order by in a subquery
SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i);
SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i LIMIT 1);

-- both should fail
SELECT * FROM integers WHERE i=(SELECT 1, 2); 
SELECT * FROM integers WHERE i=(SELECT i, i + 2 FROM integers);

-- both should return one column with 1, 2, 3, NULL
SELECT * FROM integers WHERE EXISTS (SELECT 1, 2);
SELECT * FROM integers WHERE EXISTS (SELECT i, i + 2 FROM integers);

SELECT (SELECT i FROM integers WHERE i=1); --1
SELECT * FROM integers WHERE i > (SELECT i FROM integers WHERE i=1); --2,3

-- 3x return one column with null, 1, 2, 3
SELECT * FROM integers WHERE EXISTS(SELECT 1) ORDER BY i;
SELECT * FROM integers WHERE EXISTS(SELECT * FROM integers) ORDER BY i;
SELECT * FROM integers WHERE EXISTS(SELECT NULL) ORDER BY i;
-- empty result
SELECT * FROM integers WHERE NOT EXISTS(SELECT * FROM integers) ORDER BY i; 

SELECT EXISTS(SELECT * FROM integers); -- true
SELECT EXISTS(SELECT * FROM integers WHERE i>10); -- false

SELECT EXISTS(SELECT * FROM integers), EXISTS(SELECT * FROM integers); -- true, true
SELECT EXISTS(SELECT * FROM integers) AND EXISTS(SELECT * FROM integers); -- true

SELECT EXISTS(SELECT EXISTS(SELECT * FROM integers)); -- true

-- adapted all cases of NULL::INTEGER -> NULL
-- 2x return one column with null, 1, 2, 3
SELECT * FROM integers WHERE 1 IN (SELECT 1) ORDER BY i;
SELECT * FROM integers WHERE 1 IN (SELECT * FROM integers) ORDER BY i;
SELECT * FROM integers WHERE 1 IN (SELECT NULL) ORDER BY i; -- empty 

-- 2x 4x null
SELECT 1 IN (SELECT NULL) FROM integers; 
SELECT NULL IN (SELECT * FROM integers) FROM integers;

SELECT CAST(SUM(i) AS BIGINT) FROM integers WHERE 1 IN (SELECT * FROM integers); -- 6

-- moved ANY into any.sql
-- moved ALL into all.sql

SELECT (SELECT (SELECT (SELECT 42))); -- 42
SELECT (SELECT EXISTS(SELECT * FROM integers WHERE i>2)) FROM integers; -- single column 4xtrue
SELECT (SELECT MAX(i) FROM integers) AS k, CAST(SUM(i) AS BIGINT) FROM integers GROUP BY k; -- 3,6

-- more all into all.sql
SELECT i % 2 AS k, CAST(SUM(i) AS BIGINT) FROM integers GROUP BY k HAVING SUM(i) > (SELECT MAX(i) FROM integers); -- 1,4
SELECT i FROM integers WHERE NOT(i IN (SELECT i FROM integers WHERE i>1)); -- 1
SELECT (SELECT CAST(SUM(i) AS BIGINT) FROM integers), (SELECT 42);

SELECT 1 FROM integers WHERE SUM(i) > 1; --aggregates not allowed in where clause

SELECT SUM(SUM(i)) FROM integers; -- aggregates cannot be nested

SELECT i1.i FROM integers i1 WHERE i1.i >= (SELECT i1.i, i2.i FROM integers i2 WHERE i2.i > 1); --error, subquery must return a single column

SELECT i1.i FROM integers i1 GROUP BY (SELECT SUM(i1.i) + i2.i FROM integers i2); --error, aggregate function not allowed in GROUP BY clause

SELECT i1.i FROM integers i1 GROUP BY (SELECT i2.i FROM integers i2); --error, column "i1.i" must appear in the GROUP BY clause or be used in an aggregate function

SELECT 1 FROM integers i1 GROUP BY (VALUES(1), (2)); --error, more than one row returned by a subquery used as an expression

SELECT 1 FROM integers i1 GROUP BY (VALUES(1,2,3)); --error, subquery must return only one column

SELECT (VALUES(1));

SELECT (VALUES(1),(2)); --error, cardinality violation, scalar value expected

SELECT (VALUES(1,2,3)); --error, subquery must return only one column

SELECT i FROM integers ORDER BY (SELECT 1);

SELECT i FROM integers ORDER BY (SELECT 2); --error, the query outputs 1 column, so not possible to order by the second projection

SELECT i FROM integers ORDER BY (SELECT -1); --error, no in the order by range

drop TABLE integers;

-- varchar tests
CREATE TABLE strings(v VARCHAR(128));
INSERT INTO strings VALUES ('hello'), ('world'), (NULL);
SELECT NULL IN (SELECT * FROM strings); -- NULL
SELECT 'hello' IN (SELECT * FROM strings); -- true
SELECT 'bla' IN (SELECT * FROM strings); -- NULL
SELECT 'bla' IN (SELECT * FROM strings WHERE v IS NOT NULL); -- false
SELECT * FROM strings WHERE EXISTS(SELECT NULL);
SELECT * FROM strings WHERE EXISTS(SELECT v FROM strings WHERE v='bla');
SELECT (SELECT v FROM strings WHERE v='hello') FROM strings;
SELECT (SELECT v FROM strings WHERE v='bla') FROM strings;

drop table strings;
