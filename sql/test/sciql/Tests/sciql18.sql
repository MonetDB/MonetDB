CREATE ARRAY tmp(
    x integer DIMENSION,
    y integer DIMENSION,
    v float) AS
SELECT x/2, y/2, avg(v) 
FROM matrix 
GROUP BY DISTINCT matrix[x:x+2][y:y+2]
WITH DATA;

CREATE ARRAY tmp2(
    x integer DIMENSION[2],
    y integer DIMENSION[2],
    v float);
INSERT INTO tmp2(v)
  SELECT avg(v) FROM matrix 
  GROUP BY DISTINCT matrix[x:x+2][y:y+2];

