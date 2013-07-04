SET SCHEMA rs;

DECLARE window_size INT;
SET window_size = 3;	-- using a 3x3 window
--SET window_size = 5;	-- using a 5x5 window


DECLARE d1 INT, d2 INT, majority INT;
SET d1 = window_size / 2;
SET d2 = d1 + 1;
SET majority = (window_size * window_size) / 2;

-- Assuming these example TIF images are stored in /tmp
-- The orthorectified images need the GDAL functions attach2() and import2()
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
-- Now the TIF images have been imported as the following:
--   the b3, b4 and b7 of the 1st image as rs.image1, rs.image2 and rs.image3
--   the b3, b4 and b7 of the 2nd image as rs.image4, rs.image5 and rs.image6

DECLARE size_x INT, size_y INT;
SET size_x = (SELECT MAX(x) + 1 FROM rs.image1);
SET size_y = (SELECT MAX(y) + 1 FROM rs.image1);

-- BSM classification (landsatFirePredicate()) using one image
CREATE ARRAY fire1 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);
INSERT INTO fire1 (
  SELECT b3.x, b3.y, 1
  FROM rs.image1 AS b3, rs.image2 AS b4, rs.image3 AS b7
  WHERE b3.x = b4.x AND b3.y = b4.y AND b3.x = b7.x AND b3.y = b7.y -- join the images
    and b3.intensity <> 0 AND b4.intensity <> 0 AND b7.intensity <> 0
    AND b4.intensity <= 60 -- indexNIR
    AND FLOOR(CAST(b3.intensity+b4.intensity AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO
    AND b4.intensity + b7.intensity <> 0.0
    AND (CAST(b4.intensity-b7.intensity AS DOUBLE)/(b4.intensity + b7.intensity) + 1.0) * 127.5 <= 126.0 -- indexNBR, 255.0/2.0=127.5
);

-- BSM classification (landsatFirePredicate()) using two images
CREATE ARRAY fire2 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);
INSERT INTO fire2 (
  SELECT img1_b3.x, img1_b3.y, 1
  FROM rs.image1 AS img1_b3, rs.image2 AS img1_b4, rs.image3 AS img1_b7,
       rs.image4 AS img2_b3, rs.image5 AS img2_b4
  WHERE img1_b3.intensity <> 0 AND img1_b4.intensity <> 0 AND img1_b7.intensity <> 0
    AND img1_b4.intensity <= 60 -- indexNIR_img1
	AND img1_b3.x = img1_b4.x AND img1_b3.y = img1_b4.y AND img1_b3.x = img1_b7.x AND img1_b3.y = img1_b7.y -- join the images
    AND FLOOR(CAST(img1_b3.intensity+img1_b4.intensity AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO_img1
    AND img1_b4.intensity + img1_b7.intensity <> 0.0
    AND (CAST(img1_b4.intensity-img1_b7.intensity AS DOUBLE)/(img1_b4.intensity + img1_b7.intensity) + 1.0) * 127.5 <= 126.0 -- indexNBR_img1
	AND img1_b3.x = img2_b3.x AND img1_b3.y = img2_b3.y AND img1_b3.x = img2_b4.x AND img1_b3.y = img2_b4.y -- join the images
    AND img1_b4.intensity + img1_b3.intensity <> 0.0 AND img2_b4.intensity + img2_b3.intensity <> 0.0
    AND ABS( CAST(img1_b4.intensity-img1_b3.intensity AS DOUBLE)/(img1_b4.intensity + img1_b3.intensity) -
             CAST(img2_b4.intensity-img2_b3.intensity AS DOUBLE)/(img2_b4.intensity + img2_b3.intensity) ) > $__ndviThreshold
);

-- BSM majority filter
INSERT INTO fire1 (
  SELECT [x], [y], 1    
  FROM fire1
  GROUP BY fire1[x-d1:x+d2][y-d1:y+d2]
  HAVING SUM(f)-f > majority
);

-- BSM clump&eliminate filter
CREATE ARRAY fire_eliminated (x INT DIMENSION[size_x], y INT DIMENSION[size_y], gid INT);

---- Use 4-connected, i.e., each pixel has 4 neighboring pixels,
----   namely North, East, South, West.
CREATE PROCEDURE count_groups_4connected()
BEGIN
  DECLARE moreupdates INT;
  SET moreupdates = 1;

  INSERT INTO fire_eliminated (
    SELECT x, y, x * size_y + y FROM fire1
    WHERE f = 1);

  WHILE moreupdates > 0 DO
    INSERT INTO fire_eliminated (
      SELECT [x], [y], MAX(gid) FROM fire_eliminated AS fe
        GROUP BY fe[x][y], fe[x+1][y], fe[x][y+1], fe[x-1][y], fe[x][y-1]);

    SELECT SUM(res) INTO moreupdates
      FROM (
        SELECT MAX(gid) - MIN(gid) AS res
          FROM fire_eliminated
          GROUP BY fe[x][y], fe[x+1][y], fe[x][y+1], fe[x-1][y], fe[x][y-1]
      ) AS updates;
  END WHILE;

---- Eliminate any groups that have few members (<10 pixels)
  DECLARE TABLE to_eliminate (gid INT);
  INSERT INTO to_eliminate (
    SELECT gid FROM fire_eliminated
    WHERE gid > 0
    GROUP BY gid HAVING COUNT(gid) < 10);

  UPDATE fire_eliminated SET gid = NULL WHERE gid IN (SELECT * FROM to_eliminate);
END;

---- Use 8-connected, i.e., each pixel has 8 neighbors, 
----   namely N, NE, E, SE, S, SW, W, NW.
CREATE PROCEDURE count_groups_8connected()
BEGIN
  DECLARE moreupdates INT;
  SET moreupdates = 1;

  INSERT INTO fire_eliminated (
    SELECT x, y, x * size_y + y FROM fire1
    WHERE f = 1
  );

  WHILE moreupdates > 0 DO
    INSERT INTO fire_eliminated (
      SELECT [x], [y], MAX(gid) FROM fire_eliminated
        GROUP BY fire_eliminated[x-1:x+2][y-1:y+2]);

    SELECT SUM(res) INTO moreupdates
      FROM (
        SELECT MAX(gid) - MIN(gid) AS res
          FROM fire_eliminated
          GROUP BY fire_eliminated[x-1:x+2][y-1:y+2]
      ) AS updates;
  END WHILE;

---- Eliminate any groups that have few members (<10 pixels)
  DECLARE TABLE to_eliminate (gid INT);
  INSERT INTO to_eliminate (
    SELECT gid FROM fire_eliminated
    WHERE gid > 0
    GROUP BY gid HAVING COUNT(gid) < 10);

  UPDATE fire_eliminated SET gid = NULL WHERE gid IN (SELECT * FROM to_eliminate);
END;

-- BSM connect nearby fires filter
---- Union fires which are less that 3 pixels apart (using 8-CONNECTED)
---- Add fire bridge between them

---- This impl. is most probably slower,
----   but it only(?) uses SciQL features implemented
CREATE PROCEDURE connect_nearby_fires1()
BEGIN
  DECLARE merge_more INT, update_more INT;
  SET merge_more = 1;
  SET update_more = 1;

  -- Denote pairs of nearby fire groups that 
  --   should be merged from gid_from into gid_into
  CREATE TABLE nearby_groups (gid_from INT, gid_into INT);

  -- This process needs to be repeated, because it can happen that one pixel is
  --   the _only_ connecting point for >2 fire groups
  WHILE merge_more > 0 DO
	-- Find pairs of nearby fire groups by checking all 8 neighbours of each
	--   non-fire pixel to see if the neighbours denoting more than one fire
	--   groups.
    INSERT INTO nearby_groups (
        SELECT DISTINCT MIN(gid), MAX(gid) FROM fire_eliminated AS fe
        GROUP BY fe[x-2:x+3][y-2:y+3] -- 5x5 window => max distance within the window = 3
        HAVING gid IS NULL -- this is a non-fire pixel
           AND MIN(gid) IS NOT NULL -- this group has at least one fire pixel
           AND MIN(gid) <> MAX(gid) -- this group has >1 fire groups
    );

    SELECT COUNT(*) INTO merge_more FROM nearby_groups;

	-- Merge fire groups by overwriting gid of the group with a smaller ID with
	--   the gid of the other group
	-- This process needs to be repeated to deal with recursive merging, e.g.,
	--   (gid_from, gid_into) = {(2, 4), (4, 5)}
    WHILE update_more > 0 DO
      INSERT INTO fire_eliminated (
        SELECT [x], [y], gid_into FROM fire_eliminated, nearby_groups
        WHERE gid = gid_from);

      SELECT COUNT(gid) INTO update_more FROM fire_eliminated, nearby_groups
      WHERE gid = gid_from;
    END WHILE;

    Dare testedELETE FROM nearby_groups WHERE gid_from IS NOT NULL; 
  END WHILE;

  -- TODO: add bridge!
END;

---- This impl. is most probably faster,
----   but it uses unimplemented SciQL features
CREATE PROCEDURE connect_nearby_fires2()
BEGIN
  -- Denote neighbours in the outer circle of a 5x5 window aroudn a non-fire pixel
  -- Only gid-s in the outer circle are sufficient, since small fire groups (in
  --   this case, contains only on fire pixel) have been eliminated by the
  --   clump&eliminate filter
  CREATE TABLE nearby_groups (
	n1 INT, n2 INT,  n3 INT,  n4 INT,  n5 INT,  n6 INT,  n7 INT,  n8 INT, 
	n9 INT, n10 INT, n11 INT, n12 INT, n13 INT, n14 INT, n15 INT, n16 INT, 
	gid_into INT);

  -- This way, we find all GIDs that need to be merged in one go
  INSERT INTO nearby_groups (
      SELECT fe[x-2][y-2].gid, fe[x-1][y-2].gid, fe[x][y-2].gid, fe[x+1][y-2].gid, fe[x+2][y-2].gid,
	         fe[x-2][y-1].gid,                                                     fe[x+2][y-1].gid,
	         fe[x-2][y].gid,                                                       fe[x+2][y].gid,
	         fe[x-2][y+1].gid,                                                     fe[x+2][y+1].gid,
			 fe[x-2][y+2].gid, fe[x-1][y+2].gid, fe[x][y+2].gid, fe[x+1][y+2].gid, fe[x+2][y+2].gid,
			 MAX(gid)
      FROM fire_eliminated AS fe
      GROUP BY fe[x-1:x+2][y-1:y+2]
      HAVING gid IS NULL -- this is a non-fire pixel
         AND MIN(gid) IS NOT NULL -- this group has at least one fire pixel
         AND MIN(gid) <> MAX(gid) -- this group has >1 fire groups
  );

  -- We overwrite all GIDs that should be merged in one go
  INSERT INTO fire_eliminated (
    SELECT [x], [y], gid_into FROM fire_eliminated, nearby_groups
    WHERE gid IS NOT NULL AND gid < gid_into
	  AND (gid = n1  OR gid = n2  OR gid = n3  OR gid = n4  OR
		   gid = n5  OR gid = n6  OR gid = n7  OR gid = n8  OR
		   gid = n9  OR gid = n10 OR gid = n11 OR gid = n12 OR
		   gid = n13 OR gid = n14 OR gid = n15 OR gid = n16)
  );

  -- TODO: add bridge!
END;


-- ALTERNATIVE: create single array with 3 cell values (one per band) --

-- option 1:
CREATE ARRAY image123 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT DEFAULT 0, b4 INT DEFAULT 0, b7 INT DEFAULT 0);
INSERT INTO image123 (
  SELECT b3.x, b3.y, b3.intensity, b4.intensity, b7.intensity
  FROM rs.image1 AS b3, rs.image2 AS b4, rs.image3 AS b7
  WHERE b3.x = b4.x AND b3.y = b4.y AND b3.x = b7.x AND b3.y = b7.y
);

-- option 2:
CREATE ARRAY image347 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT DEFAULT 0, b4 INT DEFAULT 0, b7 INT DEFAULT 0);
UPDATE image347 SET b3 = ( SELECT intensity FROM rs.image1 as i WHERE image347.x = i.x and image347.y = i.y );
UPDATE image347 SET b4 = ( SELECT intensity FROM rs.image2 as i WHERE image347.x = i.x and image347.y = i.y );
UPDATE image347 SET b7 = ( SELECT intensity FROM rs.image3 as i WHERE image347.x = i.x and image347.y = i.y );

