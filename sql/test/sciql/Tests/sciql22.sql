CREATE ARRAY landsat ( channel integer DIMENSION[7], x integer DIMENSION[1024], y integer DIMENSION[1024], v integer);
UPDATE landsat SET v = noise(v,delta) WHERE channel = 6 and mod(x,6) = 1;

CREATE FUNCTION tvi (b3 REAL, b4 REAL) RETURNS REAL
RETURN POWER( ((b4 - b3)/ (b4 + b3) + 0.5), 0.5);

CREATE FUNCTION conv (
    a ARRAY(i INTEGER DIMENSION[3], j INTEGER DIMENSION[3], v FLOAT))
RETURNS FLOAT
BEGIN
  DECLARE s1 FLOAT, s2 FLOAT, z FLOAT;
  SET s1 = (a[0][0].v + a[0][2].v +
            a[2][0].v + a[2][2].v)/4.0;
  SET s2 = (a[0][1].v + a[1][0].v +
            a[1][2].v + a[2][1].v)/4.0;
  SET z = 2 * ABS(s1 - s2);
  IF ((ABS(a[1][1].v - s1)> z) or (ABS(a[1][1].v - s2)> z))
  THEN RETURN s2;
  ELSE RETURN a[1][1].v;
  END IF;
END;

SELECT [x], [y],
  tvi( conv(landsat[3][x-1:x+2][y-1:y+2]),
       conv(landsat[4][x-1:x+2][y-1:y+2]))
FROM landsat;

CREATE FUNCTION conv2 (
    a ARRAY (i INTEGER DIMENSION[3], j INTEGER DIMENSION[3], v FLOAT))
RETURNS FLOAT
BEGIN
  DECLARE s1 FLOAT, s2 FLOAT, z FLOAT;
  SET s1 = (SELECT AVG(v) FROM a
            WHERE a.i = 1 AND a.j = 1
            GROUP BY a[i-1][j-1], a[i-1][j+1],
                     a[i+1][j-1], a[i+1][j+1]);
  SET s2 = (SELECT AVG(v) FROM a
            WHERE a.i = 1 AND a.j = 1
            GROUP BY a[i-1][j], a[i][j-1], a[i][j+1], a[i+1][j]);
  SET z = 2 * ABS(s1 - s2);
  IF ((ABS(a[1][1].v - s1)> z) or
      (ABS(a[1][1].v - s2)> z))
  THEN RETURN s2;
  ELSE RETURN a[1][1].v;
  END IF;
END;

