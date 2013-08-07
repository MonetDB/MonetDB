-- create an unbounded array
--! CREATE ARRAY ary (x INTEGER DIMENSION[*:3:*], v FLOAT DEFAULT 3.7);
--! SELECT * FROM ary;
--! DROP ARRAY ary;

-- UNDEFINED because you don't know the starting point
CREATE TABLE ary (x INTEGER, v FLOAT DEFAULT 3.7);
SELECT * FROM ary;
DROP TABLE ary;
