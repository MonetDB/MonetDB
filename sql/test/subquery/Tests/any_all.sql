
SELECT 1 = ANY(SELECT 1); -- true
SELECT 1 = ANY(SELECT NULL); -- NULL
SELECT 1 = ANY(SELECT 2); -- false
SELECT NULL = ANY(SELECT 2); -- NULL

SELECT 1 = ALL(SELECT 1); -- true
SELECT 1 = ALL(SELECT NULL); -- NULL
SELECT 1 = ALL(SELECT 2); -- false
SELECT NULL = ALL(SELECT 2); -- NULL

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3);

-- ANY is like EXISTS without NULL values
SELECT 2 > ANY(SELECT * FROM integers); -- true
SELECT 1 > ANY(SELECT * FROM integers); -- false

SELECT 4 > ALL(SELECT * FROM integers); -- true
SELECT 1 > ALL(SELECT * FROM integers); -- false

-- NULL input always results in NULL output
SELECT NULL > ANY(SELECT * FROM integers); -- NULL
SELECT NULL > ALL(SELECT * FROM integers); -- NULL

-- now with a NULL value in the input
INSERT INTO integers VALUES (NULL);

-- ANY returns either true or NULL
SELECT 2 > ANY(SELECT * FROM integers); -- true
SELECT 1 > ANY(SELECT * FROM integers); -- NULL

-- ALL returns either NULL or false
SELECT 4 > ALL(SELECT * FROM integers); -- NULL
SELECT 1 > ALL(SELECT * FROM integers); -- false

-- NULL input always results in NULL
SELECT NULL > ANY(SELECT * FROM integers); -- NULL
SELECT NULL > ALL(SELECT * FROM integers); -- NULL

SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;
--True
--True
--True
--False

/*Wrong results
SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;*/
--False
--False
--True
--NULL

/*Wrong results
SELECT i=ALL(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;*/
--False
--False
--False
--True

SELECT i FROM integers i1 WHERE i=ANY(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;
--1
--2
--3

/*BROKEN
SELECT i FROM integers i1 WHERE i<>ANY(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;*/
-- (Empty result set)

SELECT i FROM integers i1 WHERE i=ANY(SELECT i FROM integers WHERE i<>i1.i) ORDER BY i;
-- (Empty result set)

SELECT i FROM integers i1 WHERE i>ANY(SELECT i FROM integers WHERE i<>i1.i) ORDER BY i;
--2
--3

SELECT i FROM integers i1 WHERE i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) ORDER BY i;
--3

SELECT i=ALL(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;
--True
--True
--True
--True

SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;
--True
--True
--True
--False

SELECT i<>ALL(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;
--True
--False
--False
--False

SELECT i<>ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;
--False
--False
--False
--False

SELECT i=ANY(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;
--False
--False
--False
--False

SELECT i>ANY(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;
--False
--True
--True
--False

/*Wrong results
SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers) FROM integers i1 ORDER BY i;*/
--False
--False
--NULL
--NULL

/*Wrong results
SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;*/
--False
--False
--True
--NULL

SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i OR i IS NULL) FROM integers i1 ORDER BY i;
--True
--True
--True
--NULL

/*Wrong results
SELECT i=ALL(SELECT i FROM integers WHERE i=i1.i OR i IS NULL) FROM integers i1 ORDER BY i;*/
--NULL
--NULL
--NULL
--NULL

SELECT MIN(i)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;
--False

SELECT SUM(i)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;
--True

/*BROKEN
SELECT (SELECT SUM(i)+SUM(i1.i) FROM integers)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;*/
--True

SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i AND i>10) FROM integers i1 ORDER BY i;
--False
--False
--False
--False

DROP TABLE integers;
