DECLARE size_x INT, size_y INT;
SET size_x = $img_len;
SET size_y = $img_hei;

-- Import the TIFF images
--   the b3, b4 and b7 of the 1st image as rs.image1, rs.image2 and rs.image3
--   the b3, b4 and b7 of the 2nd image as rs.image4, rs.image5 and rs.image6
-- Then, put the three bands of each image into one array:
CREATE ARRAY landsat5_img1 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
CREATE ARRAY landsat5_img2 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
INSERT INTO landsat5_img1 (
	SELECT img1.x, img1.y, img1.intensity AS b3,
	       img2.intensity AS b4, img3.intensity AS b7
	FROM rs.image1 AS img1, rs.image2 AS img2, rs.image3 AS img3
	WHERE img1.x = img2.x AND img1.x = img3.x
	  AND img1.y = img2.y AND img1.y = img3.y);
INSERT INTO landsat5_img2 (
	SELECT img4.x, img4.y, img4.intensity AS b3,
	       img5.intensity AS b4, img6.intensity AS b7
	FROM rs.image4 AS img4, rs.image5 AS img5, rs.image6 AS img6
	WHERE img4.x = img5.x AND img4.x = img6.x
	  AND img4.y = img5.y AND img4.y = img6.y);

-- BSM classification (landsatFirePredicate()) using one image
CREATE ARRAY fire1 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);
INSERT INTO fire1 (
  SELECT x, y, 1
  FROM landsat5_img1
  WHERE b3 <> 0 AND b4 <> 0 AND b7 <> 0
    AND b4 <= 60 -- indexNIR
    AND FLOOR(CAST(b3+b4 AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO
    AND b4 + b7 <> 0.0
    AND (CAST(b4-b7 AS DOUBLE)/(b4 + b7) + 1.0) * 127.0 <= 126.0 -- indexNBR, 255.0/2.0=127.0
);

-- BSM classification (landsatFirePredicate()) using two images
CREATE ARRAY fire2 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);
INSERT INTO fire2 (
  SELECT img1.x, img1.y, 1
  FROM landsat5_img1 AS img1, landsat5_img2 AS img2
  WHERE img1.b3 <> 0 AND img1.b4 <> 0 AND img1.b7 <> 0
    AND img1.b4 <= 60 -- indexNIR_img1
    AND FLOOR(CAST(img1.b3+img1.b4 AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO_img1
    AND img1.b4 + img1.b7 <> 0.0
    AND (CAST(img1.b4-img1.b7 AS DOUBLE)/(img1.b4 + img1.b7) + 1.0) * 127.0 <= 126.0 -- indexNBR_img1
    AND img1.b4 + img1.b3 <> 0.0 AND img2.b4 + img2.b3 <> 0.0
    AND ABS( CAST(img1.b4-img1.b3 AS DOUBLE)/(img1.b4 + img1.b3) -
             CAST(img2.b4-img2.b3 AS DOUBLE)/(img2.b4 + img2.b3) ) > $__ndviThreshold
    AND img1.x = img2.x AND img1.y = img2.y
);

-- BSM cloud-water mask filter
---- TODO

-- BSM majority filter
DECLARE half_wsize INT;
SET half_wsize = $WINDOW_SIZE/2; -- using a 3x3 or 5x5 window
CREATE ARRAY fire_marjority (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);

CREATE VIEW neighbours AS
  SELECT [x], [y], SUM(v)-v AS neighbour_cnt
  FROM fire1
  GROUP BY fire1[x-half_wsize:x+half_wsize+1][y-half_wsize:y+half_wsize+1];

INSERT INTO fire_majority (
  SELECT [f.x], [f.y], 0
  FROM fire1 AS f, neighbours AS n
  WHERE f.x = n.x AND f.y = n.y
    AND f.f = 0
    AND neighbour_cnt > half_wsize
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
    SELECT x, y, x * size_y + y FROM fire_majority
    WHERE v = 1);

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
  CREATE TABLE to_eliminate (gid INT) AS
    SELECT gid FROM fire_eliminated
	WHERE gid > 0
    GROUP BY gid HAVING COUNT(gid) < 10;

  UPDATE fire_eliminated SET gid = NULL WHERE gid IN (SELECT * FROM to_eliminate);
END;

---- Use 8-connected, i.e., each pixel has 8 neighbors, 
----   namely N, NE, E, SE, S, SW, W, NW.
CREATE PROCEDURE count_groups_8connected()
BEGIN
  DECLARE moreupdates INT;
  SET moreupdates = 1;

  INSERT INTO fire_eliminated (
    SELECT x, y, x * size_y + y FROM fire_majority
    WHERE v = 1
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
  CREATE TABLE to_eliminate (gid INT) AS
    SELECT gid FROM fire_eliminated
	WHERE gid > 0
    GROUP BY gid HAVING COUNT(gid) < 10;

  UPDATE fire_eliminated SET gid = NULL WHERE gid IN (SELECT * FROM to_eliminate);
END;

-- BSM connect nearby fires filter
---- Union fires which are less that 3 pixels apart (using 8- CONNECTED)
---- Add fire bridge between them
-- TODO

