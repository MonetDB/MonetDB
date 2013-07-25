CREATE ARRAY image_array (x INTEGER DIMENSION[3], y INTEGER DIMENSION[3], v039 FLOAT DEFAULT 0.0, v108 FLOAT DEFAULT 0.0);

SELECT [x], [y],
 CASE
  WHEN v039 > 310 AND v039 - v108 > 10 AND v039_std_dev > 4   AND v108_std_dev < 8
   THEN 2
  WHEN v039 > 310 AND v039 - v108 >  8 AND v039_std_dev > 2.5 AND v108_std_dev < 2
   THEN 1
  ELSE  0
 END AS confidence
FROM (
 SELECT [x], [y],
  v039, SQRT( v039_sqr_mean - v039_mean * v039_mean ) AS v039_std_dev,
  v108, SQRT( v108_sqr_mean - v108_mean * v108_mean ) AS v108_std_dev
 FROM (
  SELECT [x], [y],
   v039, AVG( v039 ) AS v039_mean, AVG( v039 * v039 ) AS v039_sqr_mean,
   v108, AVG( v108 ) AS v108_mean, AVG( v108 * v108 ) AS v108_sqr_mean
  FROM image_array
  GROUP BY image_array[x-1:x+2][y-1:y+2]
 ) AS tmp1
) AS tmp2;

drop array image_array;
