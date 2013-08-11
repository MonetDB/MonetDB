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

CREATE TABLE array1Dint(x INTEGER, v INTEGER DEFAULT 1);
INSERT INTO array1Dint VALUES
(0,1),
(1,1),
(2,1),
(3,1);
SELECT * FROM array1Dint;
DROP TABLE array1Dint;

-- partially bounded arrays get their implicit temporary dimension using updates
CREATE ARRAY array1Dint(x INTEGER DIMENSION[0:*:*], v INTEGER DEFAULT 1);
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

CREATE ARRAY array1Dint(x INTEGER DIMENSION[*:*:0], v INTEGER DEFAULT 1);
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

CREATE ARRAY array1Dint(x INTEGER DIMENSION[*:*:0], v INTEGER DEFAULT 1);
SELECT * FROM array1Dint;
DROP ARRAY array1Dint;

-- Relational mapping, use the same scheme as above, but with only one bound set

-- semantic errors preferrably catched
