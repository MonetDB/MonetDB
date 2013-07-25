-- Functions of DLR's Gabor filer as SciQL queries

DECLARE vct_sz INT, sz_x INT, sz_y INT;
SET vct_sz = 10;
SET sz_x = 4;
SET sz_y = 5;

---- Equivalent of the functions creating vectors and matrices
CREATE ARRAY dvector (idx INT DIMENSION[1:vct_sz+1], val DOUBLE);
CREATE ARRAY dmatrix (x INT DIMENSION[1:sz_x+1], y INT DIMENSION[1:sz_y+1], v DOUBLE);
---- Equivalent of the functions freeing allocated vectors and matrices
DROP ARRAY dvector;
DROP ARRAY dmatrix;

-- sort()
SELECT idx, val FROM dvector ORDER BY val;

-- minimun()
SELECT idx, val
FROM dvector, (SELECT min(val) AS mval FROM dvector) min_val
WHERE val = mval;

-- Mat_Abs()
UPDATE dvector SET val = ABS(val) WHERE val < 0;

-- Mat_Mean()
SELECT AVG(val) FROM dvector;

-- Mat_Vector(): turn a matrix into a vector
CREATE ARRAY mvector(idx INT DIMENSION[1:sz_x*sz_y+1], val double);
INSERT INTO mvector VALUES (
  SELECT [x * sz_y + y], v FROM dmatrix
);

-- Mat_Shift(): rotate a matrix on its both axes with $side number of positions
CREATE ARRAY shifted_dmatrix (x INT DIMENSION[1:sz_x+1], y INT DIMENSION[1:sz_y+1], v DOUBLE);
INSERT INTO shifted_dmatrix (
  SELECT CASE WHEN x<$side THEN [x+sz_x-$side] ELSE [x-$side] END AS x,
         CASE WHEN y<$side THEN [y+sz_y-$side] ELSE [y-$side] END AS y,
		 v
  FROM dmatrix
);

-- Mat_Zeros()
UPDATE dmatrix SET v = 0;

-- Mat_Copy(): copy part of one matrix into a specific place of another matrix
CREATE ARRAY copied_dmatrix (x INT DIMENSION[1:sz_x+1], y INT DIMENSION[1:sz_y+1], v DOUBLE);
INSERT INTO copied_dmatrix (
 SELECT [x+$x_target], [y+$y_target], v
 FROM dmatrix[$x_begin:$x_end+1][$y_begin:$y_end+1]
);

-- Mat_product
INSERT INTO matrixA (
  SELECT [B.x], [B.y], B.v * C.v
  FROM matrixB AS B, matrixC AS C
  WHERE B.x = C.x AND B.y = C.y
);

-- Mat_Sum
INSERT INTO matrixA (
  SELECT [B.x], [B.y], B.v + C.v
  FROM matrixB AS B, matrixC AS C
  WHERE B.x = C.x AND B.y = C.y
);

-- Mat_Substract
INSERT INTO matrixA (
  SELECT [B.x], [B.y], B.v - C.v
  FROM matrixB AS B, matrixC AS C
  WHERE B.x = C.x AND B.y = C.y
);

-- Mat_Fliplr: flip a matrix from left to right
INSERT INTO dmatrix (
  SELECT [ABS(x - sz_x)], [y], v FROM dmatrix
);

-- Mat_Flipup: flip a matrix upside down
INSERT INTO dmatrix (
  SELECT [x], [ABS(y - sz_y)], v FROM dmatrix
);

