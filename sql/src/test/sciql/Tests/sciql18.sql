CREATE ARRAY tmp(x integer DIMENSION, y integer DIMENSION, val float);
INSERT INTO tmp SELECT x, y, avg(v) 
FROM matrix 
GROUP BY DISTINCT matrix[x:x+2][y:y+2];
