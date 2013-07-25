CREATE ARRAY hrit_039_108_image_array (x INTEGER DIMENSION[3], y INTEGER DIMENSION[3], c039 SMALLINT DEFAULT 0, c108 SMALLINT DEFAULT 0);

CREATE FUNCTION val (
 value SMALLINT, slope FLOAT, voffset FLOAT, c1 FLOAT, c2 FLOAT, vc FLOAT, a FLOAT, b FLOAT
) RETURNS FLOAT BEGIN
 RETURN ( c2 * vc / LOG( c1 * vc * vc * vc / ( slope * value + voffset ) + 1 ) - b ) / a;
END;

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
  FROM (
   SELECT [x], [y],
    val( c039, 0.00365867, -0.186592, 0.0000119104, 1.43877, 2569.094, 0.9959, 3.471 ) AS v039,
    val( c108, 0.205034,  -10.4568,   0.0000119104, 1.43877,  930.659, 0.9983, 0.627 ) AS v108
   FROM hrit_039_108_image_array
  ) AS image_array
  GROUP BY image_array[x-1:x+2][y-1:y+2]
 ) AS tmp1
) AS tmp2;

DROP FUNCTION val;
DROP ARRAY hrit_039_108_image_array;
