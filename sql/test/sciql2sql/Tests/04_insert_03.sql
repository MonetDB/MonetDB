--! CREATE ARRAY matrix_0403 (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);
--! CREATE ARRAY stripes (x INT DIMENSION[4], y INT DIMENSION[4] CHECK(MOD(y,2) = 1), v FLOAT DEFAULT 6.6);
--! SELECT * FROM matrix_0403;
--! SELECT * FROM stripes;
--! INSERT INTO matrix_0403(v) SELECT v FROM stripes;
--! SELECT * FROM matrix_0403;
--! DROP ARRAY matrix_0403;

DROP TABLE stripes;
CREATE TABLE matrix_0403 (x INT CHECK(x>=0 AND x <4), y INT CHECK(y>=0 AND y <4), v FLOAT DEFAULT 0.0);
INSERT INTO matrix_0403 values
(  0, 0, 0.0 ),
(  0, 1, 1.0 ),
(  0, 2, 2.0 ),
(  0, 3, 3.0 ),

(  1, 0, 4.0 ),
(  1, 1, 5.0 ),
(  1, 2, 6.0 ),
(  1, 3, 7.0 ),

(  2, 0, 8.0 ),
(  2, 1, 9.0 ),
(  2, 2, 10.0 ),
(  2, 3, 11.0 ),

(  3, 0, 12.0 ),
(  3, 1, 13.0 ),
(  3, 2, 14.0 ),
(  3, 3, 15.0 );
CREATE TABLE stripes (x INT CHECK(x>=0 AND x<4), y INT CHECK(y>=0 AND y<4 AND MOD(y,2) = 1), v FLOAT DEFAULT 6.6);

INSERT INTO stripes values
(  0, 1, 6.6 ),
(  1, 1, 6.6 ),
(  2, 1, 6.6 ),
(  3, 1, 6.6 ),
(  0, 3, 6.6 ),
(  1, 3, 6.6 ),
(  2, 3, 6.6 ),
(  3, 3, 6.6 );

SELECT * FROM matrix_0403;

SELECT * FROM stripes;

INSERT INTO matrix_0403(v) SELECT v FROM stripes;
SELECT * FROM matrix_0403;
--! (  0, 0, 0.0 ),
--! (  0, 1, 6.6 ),
--! (  0, 2, 2.0 ),
--! (  0, 3, 6.6 ),
--! 
--! (  1, 0, 4.0 ),
--! (  1, 1, 6.6 ),
--! (  1, 2, 6.0 ),
--! (  1, 3, 6.6 ),
--! 
--! (  2, 0, 8.0 ),
--! (  2, 1, 6.6 ),
--! (  2, 2, 10.0 ),
--! (  2, 3, 6.6 ),
--! 
--! (  3, 0, 12.0 ),
--! (  3, 1, 6.6 ),
--! (  3, 2, 13.0 ),
--! (  3, 3, 6.6 );
DROP TABLE matrix_0403;
DROP TABLE stripes;

