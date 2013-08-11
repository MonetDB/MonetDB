-- 1 Dimensional range array with default step size for INTEGER
CREATE ARRAY array1Dr(x INTEGER DIMENSION[0:1:4], v INTEGER DEFAULT 1);
SELECT * FROM array1Dr;
DROP ARRAY array1Dr;

-- relational equivalent , step size can be cast as constraint
CREATE TABLE array1Dr(x INTEGER CHECK(x >=0 and x < 4 and x % 1 = 0), v INTEGER DEFAULT 1);
INSERT INTO array1Dr values 
( 0, 1 ),
( 1, 1 ),
( 2, 1 ),
( 3, 1 );
SELECT * FROM array1Dr;
DROP ARRAY array1Dr;

-- 1 Dimensional range array with default step size
CREATE ARRAY array1Dr(x INTEGER DIMENSION[-4:1:2], v INTEGER DEFAULT 1);
SELECT * FROM array1Dr;
DROP ARRAY array1Dr;

-- relational equivalent 
CREATE TABLE array1Dr(x INTEGER CHECK(x >= -4 and x < 2 and x % 1 = 0), v INTEGER DEFAULT 1);
INSERT INTO array1Dr values 
( -4, 1 ),
( -3, 1 ),
( -2, 1 ),
( -1, 1 ),
(  0, 1 ),
(  1, 1 );
SELECT * FROM array1Dr;
DROP ARRAY array1Dr;

-- 1 Dimensional range array with fixed step size
CREATE ARRAY array1Dr(x INTEGER DIMENSION[0:2:4], v INTEGER DEFAULT 1);
SELECT * FROM array1Dr;
DROP ARRAY array1Dr;

-- relational equivalent 
CREATE TABLE array1Dr(x INTEGER CHECK(x >=0 and x < 4 and x % 2 = 0), v INTEGER DEFAULT 1);
INSERT INTO array1Dr values 
( 0, 1 ),
( 2, 1 );
SELECT * FROM array1Dr;
DROP ARRAY array1Dr;

-- semantic errors preferrably catched
CREATE ARRAY array1Dr(x INTEGER DIMENSION[1:1:1], v INTEGER DEFAULT 1);
CREATE ARRAY array1Dr(x INTEGER DIMENSION[1:0:4], v INTEGER DEFAULT 1);
CREATE ARRAY array1Dr(x INTEGER DIMENSION[4:1:0], v INTEGER DEFAULT 1);
CREATE ARRAY array1Dr(x INTEGER DIMENSION[0:-1:4], v INTEGER DEFAULT 1);
CREATE ARRAY array1Dr(x INTEGER DIMENSION[-4:-1:0], v INTEGER DEFAULT 1);

