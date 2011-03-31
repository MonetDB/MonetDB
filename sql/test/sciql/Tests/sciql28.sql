CREATE TABLE events ( x int, y int);

CREATE ARRAY ximage ( x integer DIMENSION, y integer DIMENSION, v integer DEFAULT 0);
INSERT INTO ximage  SELECT [x], [y], count(*) FROM events GROUP BY x, y;

SELECT [x/16], [y/16], sum(v) 
FROM ximage 
GROUP BY DISTINCT ximage[x:x+16][y:y+16];
