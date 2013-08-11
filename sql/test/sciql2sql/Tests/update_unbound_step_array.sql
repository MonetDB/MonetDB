-- unbounded step size manipulations 
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:*:4], v INTEGER DEFAULT 1);
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

-- relational equivalent use a general dimension table to administer the valid bounds
-- alternative generic dimension representation, works for any type, requires view updates
CREATE TABLE array1Dintdim(idx INTEGER PRIMARY KEY, x INTEGER );
CREATE TABLE array1Dintval(idx INTEGER REFERENCES array1Dintdim(idx), v INTEGER DEFAULT 1);
CREATE VIEW array1Dint 
AS SELECT x,v FROM array1Dintdim, array1Dintval WHERE array1Dintdim.idx = array1Dintval.idx;

INSERT INTO array1Dintdim VALUES (0,0),(1,4);
INSERT INTO array1Dintval VALUES (0,1),(1,1);
SELECT * FROM array1Dintdim;
DROP VIEW array1Dint;
DROP TABLE array1Dintval;
DROP TABLE array1Dintdim;

-- INSERT, UPDATE and DELETE cells
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:*:4], v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES(2,2);
SELECT * FROM array1Dint;
UPDATE array1Dint SET v = 44 WHERE x < 2;
SELECT * FROM array1Dint;
DELETE FROM array1Dint WHERE x = 2; 
DELETE FROM array1Dint WHERE x = 3; 
DROP ARRAY array1Dint;

-- relational equivalent use a general dimension table to administer the valid bounds
CREATE TABLE array1Dintdim(idx INTEGER PRIMARY KEY, x INTEGER );
CREATE TABLE array1Dintval(idx INTEGER REFERENCES array1Dintdim(idx), v INTEGER DEFAULT 1);
CREATE VIEW array1Dint 
AS SELECT x,v FROM array1Dintdim, array1Dintval WHERE array1Dintdim.idx = array1Dintval.idx;

INSERT INTO array1Dintdim VALUES (0,0),(1,4);
INSERT INTO array1Dintval VALUES (0,1),(1,1);

-- insert the missing element
INSERT INTO array1Dintdim VALUES(2,2);
INSERT INTO array1Dintval VALUES(2,2);
SELECT * FROM array1Dint ORDER BY x ;

-- update all valid cells
UPDATE array1Dintval SET v = 44 WHERE idx in (SELECT idx FROM array1Dintdim WHERE x <2);
SELECT * FROM array1Dint ORDER BY x ;

-- delete cells
DELETE FROM array1Dintval WHERE idx in (SELECT idx FROM array1Dintdim WHERE x = 2 AND x < 4); 
DELETE FROM array1Dintdim WHERE x = 2 AND x < 4;
DELETE FROM array1Dintval WHERE idx in (SELECT idx FROM array1Dintdim WHERE x = 3 AND x < 4); 
DELETE FROM array1Dintdim WHERE x = 3 AND x < 4;
SELECT * FROM array1Dint ORDER BY x ;

DROP VIEW array1Dint;
DROP TABLE array1Dintval;
DROP TABLE array1Dintdim;
-- Relational mapping, use the same scheme as above, but with only one bound set

-- semantic errors preferrably catched
