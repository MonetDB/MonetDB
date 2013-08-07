-- create an array with <check constraint definition> on one of its dimensions
--! CREATE ARRAY stripes (x INT DIMENSION[4], y INT DIMENSION[4] CHECK(MOD(y,2) = 1), v FLOAT DEFAULT 0.0);
--! SELECT * FROM stripes;
--! DROP ARRAY stripes;

CREATE TABLE stripes (x INT CHECK(x>=0 AND x <4), y INT CHECK(x>=0 AND x<4 AND MOD(y,2) = 1), v FLOAT DEFAULT 0.0);
INSERT INTO stripes values
(  0, 1, 0.0 ),
(  0, 3, 0.0 ),

(  1, 1, 0.0 ),
(  1, 3, 0.0 ),

(  2, 1, 0.0 ),
(  2, 3, 0.0 ),

(  3, 1, 0.0 ),
(  3, 3, 0.0 );

SELECT * FROM stripes;
DROP TABLE stripes;
