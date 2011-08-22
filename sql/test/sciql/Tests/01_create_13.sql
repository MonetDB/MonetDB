-- create an array with multiple dimensions and multiple cell values
CREATE ARRAY ary (dimx INT DIMENSION[17:3:27], dimy INT DIMENSION[31:2:40], dimz INT DIMENSION[0:5:19], v1 FLOAT DEFAULT 0.43, v2 FLOAT DEFAULT 3.3, v3 INT DEFAULT 999);
SELECT * FROM ary;
DROP ARRAY ary;

