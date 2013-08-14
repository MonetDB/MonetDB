-- unbounded dimension take their bounds by updates
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

-- relational equivalent 
CREATE TABLE array1Dunbound(x INTEGER, v INTEGER DEFAULT 1);
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

-- unbounded arrays get their implicit temporary dimension using updates
-- shown by extending the previous one
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

-- the bounds can be stretched to accomodate more elements
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
INSERT INTO array1Dunbound VALUES (5,5);
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

-- relational equivalent, fill it to generate the complete picture
CREATE TABLE array1Dunbound(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
-- implicitly added, fill it
INSERT INTO array1Dunbound VALUES (1,1),(2,1);
INSERT INTO array1Dunbound VALUES (5,5);
-- implicitly added, fill it
INSERT INTO array1Dunbound VALUES (4,1);
SELECT * FROM array1Dunbound ORDER BY x;
DROP ARRAY array1Dunbound;

-- The bounds can be reduced by trimming them producing a [0:1:2] dimension
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
DELETE FROM array1Dunbound WHERE x = 3;
SELECT * FROM array1Dunbound;
DROP ARRAY array1Dunbound;

CREATE TABLE array1Dunbound(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dunbound VALUES (0,0),(3,3);
-- implicitly added, fill it
INSERT INTO array1Dunbound VALUES (1,1),(2,1);
DELETE FROM array1Dunbound WHERE x = 3;
SELECT * FROM array1Dunbound ORDER BY x;
DROP ARRAY array1Dunbound;

-- semantic errors preferrably catched
CREATE ARRAY array1Dunbound(x INTEGER DIMENSION, v INTEGER DEFAULT 1);

