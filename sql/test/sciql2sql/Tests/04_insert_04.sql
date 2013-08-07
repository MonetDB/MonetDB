--! CREATE ARRAY stripes (x INT DIMENSION[4], y INT DIMENSION[4] CHECK(MOD(y,2) = 1), v FLOAT DEFAULT 0.0);
--! SELECT * FROM stripes;
-- not in the paper
--! INSERT INTO stripes VALUES(1,1,25);
--! SELECT * FROM stripes;
--! DROP ARRAY stripes;

CREATE TABLE stripes (x INT CHECK(x>=0 AND x <4), y INT CHECK(y>=0 AND y <4 AND MOD(y,2) = 1), v FLOAT DEFAULT 0.0);
SELECT * FROM stripes;
-- not in the paper
INSERT INTO stripes VALUES(1,1,25);
SELECT * FROM stripes;
--! (  0, 1, 0.0 ),
--! (  1, 1, 0 ),
--! (  2, 1, 0.0 ),
--! (  3, 1, 0.0 ),
--! (  0, 3, 0.0 ),
--! (  1, 3, 0.0 ),
--! (  2, 3, 0.0 ),
--! (  3, 3, 0.0 );

DROP TABLE stripes;

