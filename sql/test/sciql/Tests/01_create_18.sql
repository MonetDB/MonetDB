-- arrays may have zero non-dimensional attribute
CREATE ARRAY ary(x INTEGER DIMENSION[1:1:10]);
SELECT * FROM ary;
DROP ARRAY ary;

