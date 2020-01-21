SELECT 1+(SELECT 1); -- 2
SELECT 1=(SELECT 1); -- true
SELECT 1<>(SELECT 1); -- false
SELECT 1=(SELECT NULL); -- NULL
SELECT NULL=(SELECT 1); -- NULL

SELECT EXISTS(SELECT 1); -- true

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

SELECT EXISTS(SELECT 1) FROM integers; -- true, true, true, true
SELECT EXISTS(SELECT * FROM integers); -- true
SELECT EXISTS(SELECT * FROM integers WHERE i IS NULL); -- true
DROP TABLE integers;

SELECT 1 IN (SELECT 1); -- true
SELECT NULL IN (SELECT 1); -- NULL
SELECT 1 IN (SELECT NULL); -- NULL
SELECT 1 IN (SELECT 2); -- false

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3);

SELECT 4 IN (SELECT * FROM integers); -- false
SELECT 1 IN (SELECT * FROM integers); -- true
SELECT 1 IN (SELECT * FROM integers) FROM integers; -- true, true, true
INSERT INTO integers VALUES (NULL); 
SELECT 4 IN (SELECT * FROM integers); -- NULL
SELECT 1 IN (SELECT * FROM integers); -- true
SELECT * FROM integers WHERE (4 IN (SELECT * FROM integers)) IS NULL ORDER BY 1; -- NULL, 1, 2, 3
SELECT * FROM integers WHERE (i IN (SELECT * FROM integers)) IS NULL ORDER BY 1; -- NULL

DROP TABLE integers;
