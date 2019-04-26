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

SELECT SUM(i) FROM integers WHERE 1 IN (SELECT * FROM integers); -- 6

-- moved ANY into any.sql
-- moved ALL into all.sql

SELECT (SELECT (SELECT (SELECT 42))); -- 42
SELECT (SELECT EXISTS(SELECT * FROM integers WHERE i>2)) FROM integers; -- single column 4xtrue
SELECT (SELECT MAX(i) FROM integers) AS k, SUM(i) FROM integers GROUP BY k; -- 3,6

-- more all into all.sql
SELECT i % 2 AS k, SUM(i) FROM integers GROUP BY k HAVING SUM(i) > (SELECT MAX(i) FROM integers); -- 1,4
SELECT i FROM integers WHERE NOT(i IN (SELECT i FROM integers WHERE i>1)); -- 1
SELECT (SELECT SUM(i) FROM integers), (SELECT 42);

drop TABLE integers;

-- varchar tests
CREATE TABLE strings(v VARCHAR(128));
INSERT INTO strings VALUES ('hello'), ('world'), (NULL);
SELECT NULL IN (SELECT * FROM strings); -- NULL
SELECT 'hello' IN (SELECT * FROM strings); -- true
SELECT 'bla' IN (SELECT * FROM strings); -- NULL
SELECT 'bla' IN (SELECT * FROM strings WHERE v IS NOT NULL); -- false
drop table strings;
