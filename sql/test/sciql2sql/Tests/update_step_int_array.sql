-- Update of arrays with unspecified final bounds
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
-- update of intermediates is straightforward
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:1:*], v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES(3,2); -- extend the bound
SELECT * FROM array1Dint;
UPDATE array1Dint SET v= 44;
SELECT * FROM array1Dint;
DELETE FROM array1Dint WHERE x =2;
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

CREATE TABLE array1Dint(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES (0,1);
INSERT INTO array1Dint VALUES(3,2); -- extend the bound
-- fill it up
INSERT INTO array1Dint VALUES (1,1), (2,1);
SELECT * FROM array1Dint ORDER BY x;
UPDATE array1Dint SET v= 44;
SELECT * FROM array1Dint ORDER BY x;
UPDATE array1Dint SET v = 1 WHERE x =2;
SELECT * FROM array1Dint ORDER BY x;
DROP TABLE array1Dint;

-- semantic errors preferrably catched
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:1:4], v INTEGER DEFAULT 1); 
DROP ARRAY array1Dint;
