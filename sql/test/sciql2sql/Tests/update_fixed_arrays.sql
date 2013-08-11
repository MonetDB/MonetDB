-- Update of a fixed array, initialization first
CREATE ARRAY array1D(x INTEGER DIMENSION[4], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D VALUES(0,2,2);
SELECT * FROM array1D;
-- overwrite last value
INSERT INTO array1D VALUES(0,3,3);
SELECT * FROM array1D;
DROP ARRAY array1D;

-- relational equivalent 
CREATE TABLE array1D(x INTEGER CHECK(x >=0 and x < 4), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 0),
( 1, 1, 0),
( 2, 1, 0),
( 3, 1, 0);

--delete old cell value first
DELETE FROM array1D WHERE x = 0;
INSERT INTO array1D VALUES(0,2,2);

--delete old cell value first
DELETE FROM array1D WHERE x = 0;
INSERT INTO array1D VALUES(0,3,3);

SELECT * FROM array1D ORDER BY x;
DROP ARRAY array1D;

-- an update triggered by a query
CREATE ARRAY array1D(x INTEGER DIMENSION[4], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
UPDATE array1D SET v= NULL, w= w+x;
SELECT * FROM array1D;
DROP ARRAY array1D;

-- relational equivalent 
CREATE TABLE array1D(x INTEGER CHECK(x >=0 and x < 4), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 0),
( 1, 1, 0),
( 2, 1, 0),
( 3, 1, 0);
UPDATE array1D SET v= NULL, w= w+x;
SELECT * FROM array1D;
DROP ARRAY array1D;

-- deletion non-dimensional attribute turns into a reset to default
CREATE ARRAY array1D(x INTEGER DIMENSION[4], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
DELETE FROM array1D where v = 1;
SELECT * FROM array1D;
DROP ARRAY array1D;

-- relational equivalent  turns it into reset to default
CREATE TABLE array1D(x INTEGER CHECK(x >=0 and x < 4), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 0),
( 1, 1, 0),
( 2, 1, 0),
( 3, 1, 0);
UPDATE array1D SET v = 1 where v =1;
SELECT * FROM array1D;
DROP ARRAY array1D;

-- deletion of dimension values lead to a value reset OR ERROR?a 
CREATE ARRAY array1D(x INTEGER DIMENSION[4], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
UPDATE array1D SET v= NULL, w= w+x;
SELECT * FROM array1D;
DELETE FROM array1D WHERE x =3; 
SELECT * FROM array1D;
DROP ARRAY array1D;

-- Relational equivalent
CREATE TABLE array1D(x INTEGER CHECK(x >=0 and x < 4), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
UPDATE array1D SET v= NULL, w= w+x;
SELECT * FROM array1D;
UPDATE array1D SET x=1, w= 0 WHERE x= 3;
SELECT * FROM array1D;
DROP TABLE array1D;

-- semantic errors (YET not catched)
CREATE ARRAY array1D(x INTEGER DIMENSION[4], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D VALUES(-1,1,0);
SELECT * FROM array1D;
INSERT INTO array1D VALUES(4,1,0);
SELECT * FROM array1D;
UPDATE array1D SET x = x -1;
SELECT * FROM array1D;

UPDATE array1D SET x= 2 WHERE x=1; -- no assignment to dimension in fixed array  
SELECT * FROM array1D;
DROP ARRAY array1D;
