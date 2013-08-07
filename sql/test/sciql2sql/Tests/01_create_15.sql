-- create an array with <check constraint definition> on one of its dimensions
--! CREATE ARRAY diagonal(x INTEGER DIMENSION[4], y INTEGER DIMENSION[4] CHECK(x = y), v FLOAT DEFAULT 0.0);
--! SELECT * FROM diagonal;
--! DROP ARRAY diagonal;

CREATE TABLE diagonal (x INT CHECK(x>=0 AND x <4), y INT CHECK(x>=0 AND x<4 AND x=y), v FLOAT DEFAULT 0.0);
INSERT INTO diagonal values
(  0, 0, 0.0 ),
(  1, 1, 0.0 ),
(  2, 2, 0.0 ),
(  3, 3, 0.0 );

SELECT * FROM diagonal;
DROP TABLE diagonal;

