-- create an unbounded array
--! CREATE ARRAY ary (x INTEGER DIMENSION[*], v FLOAT DEFAULT 3.7);
--! SELECT * FROM ary;
--! DROP ARRAY ary;

-- Every dimension value is allowed. Every step size is allowed.
CREATE TABLE ary (x INTEGER, v FLOAT DEFAULT 3.7);
SELECT * FROM ary;
DROP TABLE ary;

