-- create an array with <check constraint definition> on one of its cell values
--! CREATE ARRAY sparse( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 CHECK(v>=0.0));
--! DROP ARRAY sparse;
--! SELECT * FROM sparse;

CREATE TABLE sparse( x integer CHECK(x>=0 AND x <4), y integer CHECK(y>=0 AND y <4), v float DEFAULT 0.0 CHECK(v>=0.0));
INSERT INTO sparse values
(  0, 0, 0.0 ),
(  0, 1, 0.0 ),
(  0, 2, 0.0 ),
(  0, 3, 0.0 ),

(  1, 0, 0.0 ),
(  1, 1, 0.0 ),
(  1, 2, 0.0 ),
(  1, 3, 0.0 ),

(  2, 0, 0.0 ),
(  2, 1, 0.0 ),
(  2, 2, 0.0 ),
(  2, 3, 0.0 ),

(  3, 0, 0.0 ),
(  3, 1, 0.0 ),
(  3, 2, 0.0 ),
(  3, 3, 0.0 );

SELECT * FROM sparse;
DROP TABLE sparse;

