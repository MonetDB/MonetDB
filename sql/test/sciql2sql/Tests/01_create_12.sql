-- create an unbounded array
--! CREATE ARRAY ary (x INTEGER DIMENSION[3:*:*], v FLOAT DEFAULT 3.7);
--! SELECT * FROM ary;
--! DROP ARRAY ary;

CREATE TABLE ary (x INTEGER CHECK(x >= 3), v FLOAT DEFAULT 3.7);
SELECT * FROM ary;
DROP TABLE ary;

