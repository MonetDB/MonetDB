-- Queries used by the GameOfLife demo

-- Create bord
CREATE ARRAY life ( 
	  x INT DIMENSION [20], 
	  y INT DIMENSION [15], 
	  v INT DEFAULT 0);

-- Exploder
INSERT INTO life VALUES 
  (8, 5, 1), (8, 6, 1), (8, 7, 1), 
  (8, 8, 1), (8, 9, 1), 
  (10, 5, 1), (10, 9, 1), 
  (12, 5, 1), (12, 6, 1), (12, 7, 1), 
  (12, 8, 1), (12, 9, 1);

-- Next
CREATE VIEW neighbours AS 
  SELECT [x], [y], 
         SUM(v)-v AS neighbour_cnt 
    FROM life 
    GROUP BY life[x-1:x+2][y-1:y+2];

INSERT INTO life ( 
  SELECT [l.x], [l.y], 0 
    FROM life AS l, neighbours AS n 
    WHERE l.x = n.x AND l.y = n.y 
      AND v = 1 
      AND (neighbour_cnt < 2 OR 
           neighbour_cnt > 3) 
  UNION 
  SELECT [l.x], [l.y], 1 
    FROM life AS l, neighbours AS n 
    WHERE l.x = n.x AND l.y = n.y 
      AND v = 0 
      AND neighbour_cnt =3 
);

-- Clear
UPDATE life SET v = 0 WHERE v > 0;

-- Multigroups
INSERT INTO life VALUES 
  (5,8,1), (5,9,1), 
  (6,9,1), (6,10,1), 
  (7,11,1),  
  (13,5,1), (13,9,1), (14,6,1);

-- CountGroups
DECLARE width INT, height INT;
SET width  = (SELECT MAX(x) +1 FROM life);
SET height = (SELECT MAX(y) +1 FROM life);

CREATE FUNCTION count_groups()
RETURNS TABLE (groupid INT, cnt INT)
BEGIN
  DECLARE moreupdates INT;
  SET moreupdates = 1;

  CREATE ARRAY tmp ( 
    x   INT DIMENSION[width], 
    y   INT DIMENSION[height], 
    gid INT);
  INSERT INTO tmp (
    SELECT x, y, x * height + y FROM life 
    WHERE v = 1);

  WHILE moreupdates > 0 DO
    INSERT INTO tmp (
      SELECT [x], [y], MAX(gid) FROM tmp 
        GROUP BY tmp[x-1:x+2][y-1:y+2] 
        HAVING gid IS NOT NULL);

    SELECT SUM(res) INTO moreupdates 
      FROM (
        SELECT MAX(gid) - MIN(gid) AS res 
          FROM tmp
          GROUP BY tmp[x-1:x+2][y-1:y+2] 
          HAVING gid IS NOT NULL 
      ) AS updates;
  END WHILE; 

  RETURN SELECT gid, COUNT(gid) AS cnt 
           FROM tmp WHERE gid > 0 
           GROUP BY gid ORDER BY gid;
END;

SELECT * FROM count_groups();

DROP function count_groups;


---- Not implemented yet
-- Flip
SELECT [y] AS x, [x] AS y, life[x][y].v FROM life;

-- ReduceBordSize
ALTER ARRAY life ALTER x SET DIMENSION [1:1:4];
ALTER ARRAY life ALTER y SET DIMENSION [1:1:4];

-- EnlargeBoardSize
ALTER ARRAY life ALTER x SET DIMENSION [-1:1:7];
ALTER ARRAY life ALTER y SET DIMENSION [-1:1:7];

