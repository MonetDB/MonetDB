CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

SELECT i FROM integers WHERE i >= ALL(SELECT i FROM integers); -- empty
SELECT i, i >= ALL(SELECT i FROM integers) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, false
	-- 2, false
	-- 3, NULL
SELECT i FROM integers WHERE i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL); -- 3
SELECT i, i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, false
	-- 2, false
	-- 3, true
SELECT i FROM integers WHERE i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL); -- 3
SELECT i FROM integers WHERE i > ALL(SELECT MIN(i) FROM integers); -- 2, 3
SELECT i FROM integers WHERE i < ALL(SELECT MAX(i) FROM integers); -- 1, 2
SELECT i FROM integers WHERE i <= ALL(SELECT i FROM integers); -- empty
SELECT i FROM integers WHERE i <= ALL(SELECT i FROM integers WHERE i IS NOT NULL); -- 1
SELECT i FROM integers WHERE i = ALL(SELECT i FROM integers WHERE i=1); -- 1
SELECT i FROM integers WHERE i <> ALL(SELECT i FROM integers WHERE i=1); -- 2, 3
SELECT i FROM integers WHERE i = ALL(SELECT i FROM integers WHERE i IS NOT NULL); -- empty
SELECT i FROM integers WHERE i <> ALL(SELECT i FROM integers WHERE i IS NOT NULL); -- empty
-- zero results always results in TRUE for ALL, even if "i" is NULL
SELECT i FROM integers WHERE i <> ALL(SELECT i FROM integers WHERE i>10) ORDER BY i; -- null, 1, 2, 3
SELECT i, i <> ALL(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i; 
	-- null, true
	-- 1, true
	-- 2, true
	-- 3, true
-- zero results always results in FALSE for ANY
SELECT i, i > ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;  
	-- null, false
	-- 1, false
	-- 2, false
	-- 3, false
SELECT i, i = ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;
	-- null, false
	-- 1, false
	-- 2, false
	-- 3, false
SELECT i, i >= ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;
	-- null, false
	-- 1, false
	-- 2, false
	-- 3, false
SELECT i, i <= ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;
	-- null, false
	-- 1, false
	-- 2, false
	-- 3, false
SELECT i, i < ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;
	-- null, false
	-- 1, false
	-- 2, false
	-- 3, false
SELECT i, i <> ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;
	-- null, false
	-- 1, false
	-- 2, false
	-- 3, false

-- subqueries in GROUP BY clause
SELECT i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) AS k, SUM(i) FROM integers GROUP BY k ORDER BY k;
	-- null, null
	-- false, 3
	-- true, 3
SELECT SUM(i) FROM integers GROUP BY (i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL)) ORDER BY 1; -- NULL, 3, 3
SELECT i >= ALL(SELECT MIN(i) FROM integers WHERE i IS NOT NULL) AS k, SUM(i) FROM integers GROUP BY k ORDER BY k; 
	-- NULL, NULL
	-- true, 6
SELECT i, SUM(CASE WHEN (i >= ALL(SELECT i FROM integers WHERE i=2)) THEN 1 ELSE 0 END) FROM integers GROUP BY i ORDER BY i;
	-- null, 0
	-- 1, 0
	-- 2, 1
	-- 3, 1

DROP TABLE integers;
