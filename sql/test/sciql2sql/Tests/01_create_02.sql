-- create an unbounded array
--! CREATE ARRAY ary (x INTEGER DIMENSION, v FLOAT DEFAULT 3.7);
--! SELECT * FROM ary;
--! DROP ARRAY ary;

-- Every dimension value is allowed. It has an undefined step size.
CREATE TABLE ary (x INTEGER, v FLOAT DEFAULT 3.7);

-- what is the semantics of insertion of v=NULL
SELECT * FROM ary;
DROP TABLE ary;

