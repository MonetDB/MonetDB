-- arrays may have zero non-dimensional attribute
--! CREATE ARRAY ary(x INTEGER DIMENSION[1:1:10]);
--! SELECT * FROM ary;
--! DROP ARRAY ary;

CREATE TABLE ary(x INTEGER CHECK (x>=1 AND X <10);
INSERT INTO ary VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9);
SELECT * FROM ary;
DROP TABLE ary;
