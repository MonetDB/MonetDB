-- LoadRemoteSensingImage
CALL rs.attach('/tmp/RemoteSensing.tiff');
CALL rs.import(2);

SELECT [x], [y], v FROM rs.image2;

-- NoWater
SELECT [x], [y], v FROM image2 WHERE v > 4;

-- Histogram
SELECT v, LOG(COUNT(*)) AS logcnt, 1 
  FROM image2 
  GROUP BY v ORDER BY v;

-- WaterCoverage
SELECT CAST(COUNT(v) AS DOUBLE) / 
       (SELECT c.width * c.length 
          FROM catalog AS c 
          WHERE imageid = 2 
       ) * 100 AS water_pct 
  FROM image2 
  WHERE v < 4;

-- ZoomIn
SELECT [x-150], [y-110], v 
  FROM image2[150:350][110:200];

-- Brighter
SELECT [x-150], [y-110], 
       CASE WHEN v+100 < 256 THEN v+100 
            ELSE 255 
       END AS v 
  FROM image2[150:350][110:200];

-- BitmaskImg
CREATE ARRAY mask( 
  x INT DIMENSION[1024],  
  y INT DIMENSION[512],  
  v SMALLINT DEFAULT 0 
);

INSERT INTO mask ( 
  SELECT x, y, 255 
    FROM image2[470:670][30:170] 
  UNION 
  SELECT x, y, 255 
    FROM image2[150:350][110:200] 
);

SELECT [x], [y], v FROM mask;

-- Overlay
SELECT [i.x], [i.y], 
       CASE WHEN m.v > 0 THEN i.v
            ELSE 255 
       END AS v 
  FROM image2 AS i, mask AS m 
  WHERE i.x = m.x AND i.y = m.y;

-- AreaOfInterest
CREATE TABLE maskt ( 
  xmin INT, xmax INT, ymin INT, ymax INT 
);
INSERT INTO maskt VALUES 
  (470,670,30,170), (150,350,110,200);

-- JOIN array and table 

SELECT [i.x], [i.y], i.v 
  FROM image2 AS i, maskt AS mt 
  WHERE i.x BETWEEN mt.xmin AND mt.xmax 
    AND i.y BETWEEN mt.ymin AND mt.ymax;

DROP TABLE maskt;

