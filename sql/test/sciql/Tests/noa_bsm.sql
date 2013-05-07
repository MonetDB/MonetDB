DECLARE size_x INT, size_y INT;
SET size_x = $img_len;
SET size_y = $img_hei;
-- BSM classification one image

CREATE ARRAY fire1 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);
CREATE ARRAY landsat5 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
---- import image into langsat5
INSERT INTO fire1 (
  SELECT x, y, 1
  FROM landsat5
  WHERE b3 <> 0 AND b4 <> 0 AND b7 <> 0
    AND b4 <= 60 -- indexNIR
    AND FLOOR(CAST(b3+b4 AS DOUBLE)/2.0) <= 50.0 -- indexALBEDO
    AND b4 + b7 <> 0.0
    AND (CAST(b4-b7 AS DOUBLE)/(b4 + b7) + 1.0) * 127.0 <= 126.0 -- indexNBR, 255.0/2.0=127.0
);

-- BSM classification two images

CREATE ARRAY fire2 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], f INT DEFAULT 0);
CREATE ARRAY img1 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
CREATE ARRAY img2 (x INT DIMENSION[size_x], y INT DIMENSION[size_y], b3 INT, b4 INT, b7 INT);
---- import image into img1 and img2
INSERT INTO fire2 (
  SELECT img1.x, img1.y, 1
  FROM img1, img2
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

-- BSM majority filter
DECLARE half_wsize INT;
SET half_wsize = 3/2; -- using a 3x3 window
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

CREATE PROCEDURE count_groups()
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
        GROUP BY fire_eliminated[x-1:x+2][y-1:y+2]
        HAVING gid IS NOT NULL);

    SELECT SUM(res) INTO moreupdates
      FROM (
        SELECT MAX(gid) - MIN(gid) AS res
          FROM fire_eliminated
          GROUP BY fire_eliminated[x-1:x+2][y-1:y+2]
          HAVING gid IS NOT NULL
      ) AS updates;
  END WHILE;

  CREATE TABLE to_eliminate (gid INT) AS
    SELECT gid FROM fire_eliminated
	WHERE gid > 0
    GROUP BY gid HAVING COUNT(gid) < 10;

  UPDATE fire_eliminated SET gid = NULL WHERE gid IN (SELECT * FROM to_eliminate);
END;

-- BSM connect nearby fires filter
-- TODO

