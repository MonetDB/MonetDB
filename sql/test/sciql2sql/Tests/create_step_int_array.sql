-- step size manipulations for integer point dimensions.
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:1:4], v INTEGER DEFAULT 1);
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

-- relational equivalent is a fixed array initialization
CREATE TABLE array1Dint(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES
(0,1),
(1,1),
(2,1),
(3,1);
SELECT * FROM array1Dint;
DROP TABLE array1Dint;

-- partially bounded arrays get their implicit temporary dimension using updates
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:1:*], v INTEGER DEFAULT 1);
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

-- we know that at least one element is defined
CREATE TABLE array1Dint(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES (0,1);
SELECT * FROM array1Dint;
DROP TABLE array1Dint;

-- appending values extend the list
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:1:*], v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES(3,2); -- extend the bound
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

CREATE TABLE array1Dint(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES (0,1);
INSERT INTO array1Dint VALUES(3,2); -- extend the bound
-- fill it up
INSERT INTO array1Dint VALUES (1,1), (2,1);
SELECT * FROM array1Dint ORDER BY x;
DROP TABLE array1Dint;

-- left bound undefined works the same way
CREATE ARRAY array1Dint(x INTEGER DIMENSION[*:1:0], v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES(-3,2); -- extend the bound
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

CREATE TABLE array1Dint(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES (0,1);
INSERT INTO array1Dint VALUES(-3,2); -- extend the bound
-- fill it up
INSERT INTO array1Dint VALUES (-2,1), (-1,1);
SELECT * FROM array1Dint ORDER BY x;
DROP TABLE array1Dint;

-- semantic errors preferrably catched
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:1:4], v INTEGER DEFAULT 1); 
INSERT INTO array1Dint VALUES (23,1); -- step violation
DROP ARRAY array1Dint;
