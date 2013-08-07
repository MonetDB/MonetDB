--! CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);
--! SELECT * FROM matrix;

--! DELETE FROM matrix WHERE x = 2;
--! SELECT * FROM matrix;

--! DROP ARRAY matrix;

CREATE TABLE matrix (x INT CHECK(x>=0 and x<4), y INT CHECK(y>=0 and y<4), v FLOAT DEFAULT 0.0);
INSERT INTO matrix values
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
SELECT * FROM matrix;

DELETE FROM matrix WHERE x = 2;
SELECT * FROM matrix;
--! (  0, 0, 0.0 ),
--! (  0, 1, 0.0 ),
--! (  0, 2, 0.0 ),
--! (  0, 3, 0.0 ),

--! (  1, 0, 0.0 ),
--! (  1, 1, 0.0 ),
--! (  1, 2, 0.0 ),
--! (  1, 3, 0.0 ),

--! (  2, 0, null ),
--! (  2, 1, null ),
--! (  2, 2, null ),
--! (  2, 3, null ),

--! (  3, 0, 0.0 ),
--! (  3, 1, 0.0 ),
--! (  3, 2, 0.0 ),
--! (  3, 3, 0.0 );

DROP TABLE matrix;

