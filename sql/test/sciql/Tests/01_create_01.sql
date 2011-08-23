-- use the [size] shortcut for integer type dimensions
CREATE ARRAY ary(x TINYINT DIMENSION[4], y BIGINT DIMENSION[-5], v FLOAT DEFAULT 3.7);
SELECT * FROM ary;
DROP ARRAY ary;

