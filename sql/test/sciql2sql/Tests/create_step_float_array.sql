-- step size manipulations for floating point dimensions.
CREATE ARRAY array1Dfloat(x FLOAT DIMENSION[0.0:0.1:0.4], v INTEGER DEFAULT 1);
SELECT * FROM array1Dfloat;
DROP ARRAY array1Dfloat;

-- relational equivalent is a fixed array initialization
CREATE TABLE array1Dfloat(x FLOAT, v INTEGER DEFAULT 1);
INSERT INTO array1Dfloat VALUES
(0.0,1),
(0.1,1),
(0.2,1),
(0.3,1);
SELECT * FROM array1Dfloat;
DROP TABLE array1Dfloat;

-- partially bounded arrays get their implicit temporary dimension using updates
CREATE ARRAY array1Dfloat(x FLOAT DIMENSION[0.0:0.1:*], v INTEGER DEFAULT 1);
SELECT * FROM array1Dfloat;
DROP ARRAY array1Dfloat;

-- we know that at least one element is defined
CREATE TABLE array1Dfloat(x FLOAT, v INTEGER DEFAULT 1);
INSERT INTO array1Dfloat VALUES (0.0,1);
SELECT * FROM array1Dfloat;
DROP TABLE array1Dfloat;

-- appending values extend the list
CREATE ARRAY array1Dfloat(x FLOAT DIMENSION[0.0:0.1:*], v INTEGER DEFAULT 1);
INSERT INTO array1Dfloat VALUES(0.3,2); -- extend the bound
SELECT * FROM array1Dfloat;
DROP ARRAY array1Dfloat;

CREATE TABLE array1Dfloat(x FLOAT, v INTEGER DEFAULT 1);
INSERT INTO array1Dfloat VALUES (0.0,1);
INSERT INTO array1Dfloat VALUES(0.3,2); -- extend the bound
-- fill it up
INSERT INTO array1Dfloat VALUES (0.1,1), (0.2,1);
SELECT * FROM array1Dfloat;
DROP TABLE array1Dfloat;

-- left bound undefined works the same way
CREATE ARRAY array1Dfloat(x FLOAT DIMENSION[*:0.1:0.0], v INTEGER DEFAULT 1);
INSERT INTO array1Dfloat VALUES(-0.3,2); -- extend the bound
SELECT * FROM array1Dfloat;
DROP ARRAY array1Dfloat;

CREATE TABLE array1Dfloat(x FLOAT, v INTEGER DEFAULT 1);
INSERT INTO array1Dfloat VALUES (0.0,1);
INSERT INTO array1Dfloat VALUES(-0.3,2); -- extend the bound
-- fill it up
INSERT INTO array1Dfloat VALUES (-0.2,1), (-0.1,1);
SELECT * FROM array1Dfloat;
DROP TABLE array1Dfloat;

-- semantic errors preferrably catched
CREATE ARRAY array1Dfloat(x FLOAT DIMENSION[0.0:0.1:0.4], v INTEGER DEFAULT 1); 
INSERT INTO array1Dfloat VALUES (0.23,); -- step violation
DROP ARRAY array1Dfloat;
