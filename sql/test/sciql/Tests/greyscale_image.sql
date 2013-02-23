-- Queries used by the Greyscale Image demo

-- LoadGreyScaleImage
CALL rs.attach('/tmp/GreyScale.tiff');
CALL rs.import(1); 

SELECT [x], [y], v FROM rs.image1;

-- InverseColor
SELECT [x], [y], 255 - v FROM image1;

-- EdgeDetection
-- the workaround
SELECT [a.x], [a.y], 
       255 - ABS(a.v*2-b.v-c.v) * 2 AS v 
	  FROM image1 AS a, image1 AS b, image1 AS c 
	  WHERE a.x -1 = b.x AND a.y    = b.y 
	    AND a.x    = c.x AND a.y -1 = c.y;

-- the real query, not implemented yet
SELECT [x], [y], 255 - ABS(a[x][y].v *2 - a[x-1][y].v - a[x][y-1].v) * 2 AS v
  FROM image1 AS a;

-- Smooth
SELECT [x], [y], 
       CAST(AVG(v) AS SMALLINT) AS v 
	  FROM image1 
	  GROUP BY image1[x-3:x+4][y-3:y+4];

-- Reduce
SELECT [x/2], [y/2], v 
  FROM image1[*:2:*][*:2:*];

-- Rotate180deg
SELECT [1023-x], [767-y], v FROM image1;

