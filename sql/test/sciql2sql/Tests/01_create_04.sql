-- create an unbounded array
--! CREATE ARRAY ary (x INTEGER DIMENSION[3:13], v FLOAT DEFAULT 3.7);
--! SELECT * FROM ary;
--! DROP ARRAY ary;

CREATE TABLE ary (x INTEGER, v FLOAT DEFAULT 3.7 CHECK(x >= 3 and x < 13);
INSERT INTO ary values
(  3, 3.7 ),
(  4, 3.7 ),
(  5, 3.7 ),
(  6, 3.7 ),
(  7, 3.7 ),
(  8, 3.7 ),
(  9, 3.7 ),
(  10, 3.7 ),
(  11, 3.7 ),
(  12, 3.7 );
SELECT * FROM ary;
DROP TABLE ary;


