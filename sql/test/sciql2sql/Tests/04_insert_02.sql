--! CREATE ARRAY matrix_0402 (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);
--! SELECT * FROM matrix_0402;
--! INSERT INTO matrix_0402 SELECT x, y, 3.3 FROM matrix_0402 WHERE x = 3;
--! SELECT * FROM matrix_0402;
--! DROP ARRAY matrix_0402;

CREATE TABLE matrix_0402 (x INT CHECK(x>=0 and x<4), y INT CHECK(y>=0 and y<4), v FLOAT DEFAULT 0.0);
INSERT INTO matrix_0402 values
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
SELECT * FROM matrix_0402;
CREATE TABLE tmp(like matrix_0402);
INSERT INTO tmp SELECT x, y, v FROM matrix_0402 WHERE x = 3;
DELETE FROM matrix_0402;
INSERT INTO matrix_0402 SELECT x, y, v FROM tmp;
DROP TABLE tmp;
SELECT * FROM matrix_0402;
--! (  3, 0, 12.0 ),
--! (  3, 1, 13.0 ),
--! (  3, 2, 14.0 ),
--! (  3, 3, 15.0 );
DROP TABLE matrix_0402;

