-- For now(?), the mitosis/mergetable optimizers are not up to handling the
-- SciQL used here, in particular conjunctive HAVING predicates, correctly
-- (or vice versa).
set optimizer='no_mitosis_pipe';


SET SCHEMA rs;


-- configuration parameters --

DECLARE window_size INT;
SET window_size = 3; -- 3 for 3x3 window, 5 for 5x5 window

DECLARE ndviThreshold DOUBLE;
SET ndviThreshold = 0; -- what is the correct value ?


-- loading data (images) --

---- Assuming these example TIF images are stored in /tmp
---- The orthorectified images need the GDAL functions attach2() and import2()
CALL rs.attach2('/tmp/img1_b3.tif');
CALL rs.attach2('/tmp/img1_b4.tif');
CALL rs.attach2('/tmp/img1_b7.tif');

CALL rs.attach2('/tmp/img2_b3.tif');
CALL rs.attach2('/tmp/img2_b4.tif');
CALL rs.attach2('/tmp/img2_b7.tif');

CALL rs.import2(1);
CALL rs.import2(2);
CALL rs.import2(3);

CALL rs.import2(4);
CALL rs.import2(5);
CALL rs.import2(6);
---- Now the TIF images have been imported as the following:
---- b3, b4 and b7 of the 1st image as rs.image1, rs.image2 and rs.image3
---- b3, b4 and b7 of the 2nd image as rs.image4, rs.image5 and rs.image6


-- global variables and array --

DECLARE d1 INT, d2 INT, majority INT;
SET d1 = window_size / 2;
SET d2 = d1 + 1;
SET majority = (window_size * window_size) / 2;

DECLARE size_x INT, size_y INT;
SET size_x = (SELECT MAX(x) + 1 FROM rs.image1);
SET size_y = (SELECT MAX(y) + 1 FROM rs.image1);

CREATE ARRAY fire (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT);

---- These two tables SHOULD be DECLARED inside the function(s),
---- but that does not seem to work (correctly) yet !??
---- Hence, we create them globally, for now ...
CREATE TABLE bridges (x INT, y INT, i INT, a INT);
CREATE TABLE trans (i INT UNIQUE, a INT, x INT);


-- BSM classification (landsatFirePredicate()) --

--- two versions; please choose one:

---- version 1: using one image
INSERT INTO fire (
  SELECT b3.x, b3.y, 1
  FROM rs.image1 AS b3, rs.image2 AS b4, rs.image3 AS b7
  WHERE b3.x = b4.x AND b3.y = b4.y AND b3.x = b7.x AND b3.y = b7.y -- join the images
    and b3.intensity <> 0 AND b4.intensity <> 0 AND b7.intensity <> 0
    AND b4.intensity <= 60 -- indexNIR
    AND FLOOR(CAST(b3.intensity+b4.intensity AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO
    AND b4.intensity + b7.intensity <> 0
    AND (CAST(b4.intensity-b7.intensity AS DOUBLE)/(b4.intensity + b7.intensity) + 1.0) * 127.5 <= 126.0 -- indexNBR, 255.0/2.0=127.5
);

---- version 2: using two images
INSERT INTO fire (
  SELECT img1_b3.x, img1_b3.y, 1
  FROM rs.image1 AS img1_b3, rs.image2 AS img1_b4, rs.image3 AS img1_b7,
       rs.image4 AS img2_b3, rs.image5 AS img2_b4
  WHERE img1_b3.x = img1_b4.x AND img1_b3.y = img1_b4.y AND img1_b3.x = img1_b7.x AND img1_b3.y = img1_b7.y -- join the images
    AND img1_b3.x = img2_b3.x AND img1_b3.y = img2_b3.y AND img1_b3.x = img2_b4.x AND img1_b3.y = img2_b4.y -- join the images
    AND img1_b3.intensity <> 0 AND img1_b4.intensity <> 0 AND img1_b7.intensity <> 0
    AND img2_b3.intensity <> 0 AND img2_b4.intensity <> 0
    AND img1_b4.intensity <= 60 -- indexNIR_img1
    AND FLOOR(CAST(img1_b3.intensity+img1_b4.intensity AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO_img1
    AND img1_b4.intensity + img1_b7.intensity <> 0
    AND (CAST(img1_b4.intensity-img1_b7.intensity AS DOUBLE)/(img1_b4.intensity + img1_b7.intensity) + 1.0) * 127.5 <= 126.0 -- indexNBR_img1
    AND img1_b4.intensity + img1_b3.intensity <> 0
    AND img2_b4.intensity + img2_b3.intensity <> 0
    AND ABS( CAST(img1_b4.intensity-img1_b3.intensity AS DOUBLE)/(img1_b4.intensity + img1_b3.intensity) -
             CAST(img2_b4.intensity-img2_b3.intensity AS DOUBLE)/(img2_b4.intensity + img2_b3.intensity) ) > ndviThreshold
);


-- BSM majority filter --

INSERT INTO fire (
  SELECT [x], [y], 1
  FROM fire
  GROUP BY fire[x-d1:x+d2][y-d1:y+d2]
  HAVING f IS NULL AND SUM(f) > majority
);


-- BSM clump&eliminate filter --

---- initialize with distinct group ID per pixel
UPDATE fire SET f = x * size_y + y WHERE f IS NOT NULL;

--- two versions; please choose one:
--- CAVEAT: these take more than 30 minutes to execute !

---- version 1:
---- Clump adjacent pixels using 4-connected,
---- i.e., each pixel has 4 neighbors: N, E, S, W
---- HOWEVER, this DOES NOT WORK (yet?), as the SciQL implementation does
---- not support structural grouping (tiling) with non-rectangular windows
---- (tiles) (yet?) !??
--CREATE FUNCTION clump_4connected()
--RETURNS INT
--BEGIN
--  DECLARE iterations INT;
--  SET iterations = 0;
--  DECLARE moreupdates INT;
--  SET moreupdates = 1;
--  WHILE moreupdates > 0 DO
--    SET iterations = iterations + 1;
--    INSERT INTO fire (
--      SELECT [x], [y], MAX(f)
--      FROM fire AS a
--      -- the SciQL implementation does not support this GROUP BY (yet?) !??
--      GROUP BY a[x][y], a[x+1][y], a[x][y+1], a[x-1][y], a[x][y-1]
--      HAVING f IS NOT NULL
--    );
--    SELECT SUM(res) INTO moreupdates
--    FROM (
--      SELECT MAX(f) - MIN(f) AS res
--      FROM fire AS a
--      -- the SciQL implementation does not support this GROUP BY (yet?) !??
--      GROUP BY a[x][y], a[x+1][y], a[x][y+1], a[x-1][y], a[x][y-1]
--      HAVING f IS NOT NULL
--    ) AS updates;
--  END WHILE;
--  RETURN iterations;
--END;
--SELECT clump_4connected();

---- version 2:
---- Clump adjacent pixels using 8-connected,
---- i.e., each pixel has 8 neighbors: N, NE, E, SE, S, SW, W, NW
CREATE FUNCTION clump_8connected()
RETURNS INT
BEGIN
  DECLARE iterations INT;
  SET iterations = 0;
  DECLARE moreupdates INT;
  SET moreupdates = 1;
  WHILE moreupdates > 0 DO
    SET iterations = iterations + 1;
    INSERT INTO fire (
      SELECT [x], [y], MAX(f)
      FROM fire
      GROUP BY fire[x-1:x+2][y-1:y+2]
      HAVING f IS NOT NULL
    );
    SELECT SUM(res) INTO moreupdates
    FROM (
      SELECT MAX(f) - MIN(f) AS res
      FROM fire
      GROUP BY fire[x-1:x+2][y-1:y+2]
      HAVING f IS NOT NULL
    ) AS updates;
  END WHILE;
  RETURN iterations;
END;
SELECT clump_8connected();

---- Eliminate any groups that have few members (<10 pixels)
UPDATE fire SET f = NULL WHERE f IN (
  SELECT f
  FROM fire
  WHERE f IS NOT NULL
  GROUP BY f
  HAVING COUNT(f) < 10
);


-- BSM connect nearby fires filter --

--- CAVEAT: this takes more than 10 minutes to execute !

---- Union fires which are less that 3 pixels apart (using 8-CONNECTED)
---- Add fire bridge between them
CREATE FUNCTION connect_neighbors()
RETURNS TABLE (i1 INT, i2 INT)
BEGIN
  --DECLARE TABLE bridges (x INT, y INT, i INT, a INT);
  --DECLARE TABLE trans (i INT UNIQUE, a INT, x INT);
  DECLARE iter_0 INT, iter_1 INT;
  SET iter_0 = 0;
  SET iter_1 = 0;
  DECLARE merge_more INT, recurse INT;
  SET merge_more = 1;
  WHILE merge_more > 0 DO
    SET iter_0 = iter_0 + 1;

    -- find neighboring fire clumps
    DELETE FROM bridges;
    INSERT INTO bridges (
      -- 3x3 window is too small and 5x5 is too large; hence,
      -- we need to union the four possible 4x4 windows ...
      SELECT x, y, MIN(f) AS i, MAX(f) AS a
      FROM fire
      GROUP BY fire[x-2:x+2][y-2:y+2]
      HAVING f IS NULL
         AND MIN(f) IS NOT NULL
         AND MIN(f) <> MAX(f)
      UNION ALL
      SELECT x, y, MIN(f) AS i, MAX(f) AS a
      FROM fire
      GROUP BY fire[x-1:x+3][y-2:y+2]
      HAVING f IS NULL
         AND MIN(f) IS NOT NULL
         AND MIN(f) <> MAX(f)
      UNION ALL
      SELECT x, y, MIN(f) AS i, MAX(f) AS a
      FROM fire
      GROUP BY fire[x-2:x+2][y-1:y+3]
      HAVING f IS NULL
         AND MIN(f) IS NOT NULL
         AND MIN(f) <> MAX(f)
      UNION ALL
      SELECT x, y, MIN(f) AS i, MAX(f) AS a
      FROM fire
      GROUP BY fire[x-1:x+3][y-1:y+3]
      HAVING f IS NULL
         AND MIN(f) IS NOT NULL
         AND MIN(f) <> MAX(f)
    );

    SELECT COUNT(*) INTO merge_more FROM bridges;
    IF merge_more > 0 THEN

      -- create "bridges"
      -- these are "big" / "wide" bridges, i.e., all pixels that are at
      -- most 2 pixels away from both neigbors
      INSERT INTO fire (
        SELECT [x], [y], i
        FROM bridges
      );

      -- create transition map
      DELETE FROM trans;
      INSERT INTO trans (i,a) (
        SELECT i, MAX(a)
        FROM bridges
        GROUP BY i
      );
      DELETE FROM bridges;

      -- calculate transitive closure
      SET recurse = 1;
      WHILE recurse > 0 DO
        SET iter_1 = iter_1 + 1;
        UPDATE trans SET x = (SELECT step.a FROM trans AS step WHERE trans.a = step.i);
        UPDATE trans SET a = x WHERE x IS NOT NULL;
        SELECT COUNT(x) INTO recurse FROM trans;
      END WHILE;

      -- connect neigboring clumps
      INSERT INTO fire (
        SELECT [fire.x], [fire.y], trans.a
        FROM fire JOIN trans
          ON fire.f = trans.i
      );
      DELETE FROM trans;

    END IF;

  END WHILE;
  RETURN SELECT iter_0, iter_1;
END;
SELECT * FROM connect_neighbors();



-- (future) ALTERNATIVE:
-- create single array with 3 cell values (one per band)
-- to avoid (expensive) joins (in particular in initial classification)

-- option 1:
CREATE ARRAY image123 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
INSERT INTO image123 (
  SELECT b3.x, b3.y, b3.intensity, b4.intensity, b7.intensity
  FROM rs.image1 AS b3, rs.image2 AS b4, rs.image3 AS b7
  WHERE b3.x = b4.x AND b3.y = b4.y AND b3.x = b7.x AND b3.y = b7.y
);

-- option 2:
CREATE ARRAY image347 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
UPDATE image347 SET b3 = ( SELECT intensity FROM rs.image1 as i WHERE image347.x = i.x and image347.y = i.y );
UPDATE image347 SET b4 = ( SELECT intensity FROM rs.image2 as i WHERE image347.x = i.x and image347.y = i.y );
UPDATE image347 SET b7 = ( SELECT intensity FROM rs.image3 as i WHERE image347.x = i.x and image347.y = i.y );

-- option 3:
CREATE ARRAY imageB347 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
INSERT INTO imageB347 (x,y,b3) SELECT [x], [y], intensity FROM rs.image1;
INSERT INTO imageB347 (x,y,b4) SELECT [x], [y], intensity FROM rs.image2;
INSERT INTO imageB347 (x,y,b7) SELECT [x], [y], intensity FROM rs.image3;

