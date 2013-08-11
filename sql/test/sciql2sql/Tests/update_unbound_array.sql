-- unbounded arrays get their implicit temporary dimension using updates
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

-- relational equivalent, fill it to generate the complete picture
CREATE TABLE array1Dunbound(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
-- implicitly added, fill it
INSERT INTO array1Dunbound VALUES (1,1),(2,1);
SELECT * FROM array1Dunbound ORDER BY x;
DROP ARRAY array1Dunbound;

-- update over existing cell is identical to fixed-array
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
SELECT * FROM array1Dunbound;
INSERT INTO array1Dunbound VALUES (0,1);
SELECT * FROM array1Dunbound;
-- update over non-initialized cell
INSERT INTO array1Dunbound VALUES (1,7);
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

-- relational equivalent, fill it to generate the complete picture
CREATE TABLE array1Dunbound(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
-- implicitly added, fill it
INSERT INTO array1Dunbound VALUES (1,1),(2,1);
SELECT * FROM array1Dunbound ORDER BY x;
-- remainings turned into an update
DELETE FROM array1Dunbound WHERE x = 0;
INSERT INTO array1Dunbound VALUES (0,1);
SELECT * FROM array1Dunbound ORDER BY x;
-- update over non-initialized cell
DELETE FROM array1Dunbound WHERE x = 1;
INSERT INTO array1Dunbound VALUES (1,7);
SELECT * FROM array1Dunbound ORDER BY x;
DROP ARRAY array1Dunbound;


-- update queries
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
UPDATE array1Dunbound SET v = NULL;
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

-- relational equivalent, fill it to generate the complete picture
CREATE TABLE array1Dunbound(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
-- implicitly added, fill it
INSERT INTO array1Dunbound VALUES (1,1),(2,1);
UPDATE array1Dunbound SET v = NULL;
SELECT * FROM array1Dunbound ORDER BY x;
DROP TABLE array1Dunbound;

-- semantic errors preferrably catched

