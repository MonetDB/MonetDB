DROP ARRAY hrit_c039_image_array;
DROP ARRAY hrit_c108_image_array;
DROP ARRAY image_array;
DROP FUNCTION val;

CREATE ARRAY hrit_c039_image_array (x INTEGER DIMENSION[123], y INTEGER DIMENSION[123], v INTEGER DEFAULT 0);
CREATE ARRAY hrit_c108_image_array (x INTEGER DIMENSION[123], y INTEGER DIMENSION[123], v INTEGER DEFAULT 0);

-- insert some large random numbers for testing purpose
INSERT INTO hrit_c039_image_array SELECT x, y, MOD(RAND(), 1000)+55 FROM hrit_c039_image_array;
INSERT INTO hrit_c108_image_array SELECT x, y, MOD(RAND(), 1000)+55 FROM hrit_c108_image_array;

CREATE FUNCTION val (
 value DOUBLE, slope DOUBLE, voffset DOUBLE, c1 DOUBLE, c2 DOUBLE, vc DOUBLE, a DOUBLE, b DOUBLE
) RETURNS DOUBLE BEGIN
 RETURN ( c2 * vc / LOG( c1 * vc * vc * vc / ( slope * value + voffset ) + 1 ) - b ) / a;
END;

CREATE ARRAY image_array (x INTEGER DIMENSION[123], y INTEGER DIMENSION[123], v039 DOUBLE DEFAULT 0, v108 DOUBLE DEFAULT 0);
INSERT INTO image_array
   SELECT c039.x, c039.y,
	-- NB: since the voffsets are negative, 'v'-s must be larger than ~55 to
	--     ensure that the parameter of LOG is a positive number; or maybe we
	--     should add an ABS to the parameter of LOG.
    val( c039.v, 0.00365867, -0.186592, 0.0000119104, 1.43877, 2569.094, 0.9959, 3.471 ) AS v039,
    val( c108.v, 0.205034,  -10.4568,   0.0000119104, 1.43877,  930.659, 0.9983, 0.627 ) AS v108
   FROM
    hrit_c039_image_array AS c039 JOIN hrit_c108_image_array AS c108
    ON c039.x = c108.x AND c039.y = c108.y
;

-- Just to have a look at the values
SELECT c039.x, c039.y, c039.v, c108.v, v039, v108
FROM hrit_c039_image_array AS c039, hrit_c108_image_array AS c108, image_array AS imga
WHERE c039.x = c108.x AND c039.y = c108.y
  AND c039.x = imga.x AND c039.y = imga.y
LIMIT 10
;

SELECT confidence, count(confidence)
FROM
(
SELECT tmp2.*, --x, y, v039, v108, v039_std_dev, v108_std_dev,
 CASE
--  WHEN v039 > 310 AND v039 - v108 > 10 AND v039_std_dev > 4   AND v108_std_dev < 8
  WHEN v039 > 300 --AND v039 - v108 > 10 AND v039_std_dev > 4   AND v108_std_dev < 8
   THEN 2
--  WHEN v039 > 310 AND v039 - v108 >  8 AND v039_std_dev > 2.5 AND v108_std_dev < 2
  WHEN v039 > 250 --AND v039 - v108 >  8 AND v039_std_dev > 2.5 AND v108_std_dev < 2
   THEN 1
  ELSE  0
 END AS confidence
FROM (
 SELECT x, y,
  v039, v039_mean, v039_sqr_mean, SQRT( ABS(v039_sqr_mean - v039_mean * v039_mean) ) AS v039_std_dev,
  v108, v108_mean, v108_sqr_mean, SQRT( ABS(v108_sqr_mean - v108_mean * v108_mean) ) AS v108_std_dev
 FROM (
  SELECT x, y,
   v039, AVG( v039 ) AS v039_mean, AVG( v039 * v039 ) AS v039_sqr_mean,
   v108, AVG( v108 ) AS v108_mean, AVG( v108 * v108 ) AS v108_sqr_mean
  FROM image_array
  GROUP BY image_array[x-1:x+2][y-1:y+2]
 ) AS tmp
) AS tmp2
--LIMIT 10
) AS tmp3
GROUP BY confidence
;

