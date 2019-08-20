
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

DROP TABLE integers;
