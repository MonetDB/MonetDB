CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

SELECT i FROM integers WHERE i <= ANY(SELECT i FROM integers); -- single column 1,2,3
SELECT i FROM integers WHERE i > ANY(SELECT i FROM integers); -- single column 2,3

SELECT i, i > ANY(SELECT i FROM integers) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, NULL
	-- 2, true
	-- 3, true
SELECT i, i > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, false
	-- 2, true
	-- 3, true
SELECT i, NULL > ANY(SELECT i FROM integers) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, NULL
	-- 2, NULL
	-- 3, NULL
SELECT i, NULL > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, NULL
	-- 2, NULL
	-- 3, NULL
SELECT i FROM integers WHERE i = ANY(SELECT i FROM integers); -- single column 1, 2, 3
SELECT i, i = ANY(SELECT i FROM integers WHERE i>2) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, false
	-- 2, false
	-- 3, true
SELECT i, i = ANY(SELECT i FROM integers WHERE i>2 OR i IS NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, NULL
	-- 2, NULL
	-- 3, true
SELECT i, i <> ANY(SELECT i FROM integers WHERE i>2) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, true
	-- 2, true
	-- 3, false
SELECT i, i <> ANY(SELECT i FROM integers WHERE i>2 OR i IS NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, true
	-- 2, true
	-- 3, NULL
SELECT i, i = ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, true
	-- 2, true
	-- 3, true
SELECT i, i = ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL AND i1.i <> 2) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, true
	-- 2, false
	-- 3, true
SELECT i, i >= ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL) FROM integers ORDER BY i;
	-- NULL, NULL
	-- 1, true
	-- 2, true
	-- 3, true
SELECT i, i >= ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL AND i1.i <> 1 LIMIT 1) FROM integers ORDER BY i; -- parse error
	-- NULL, NULL
	-- 1, false
	-- 2, true
	-- 3, true

drop table integers;
