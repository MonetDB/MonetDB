CREATE ARRAY m ( x int DIMENSION[1024], v int );
UPDATE m SET
  m[x].v = (SELECT sum(a[x][y].v * b[k].v) FROM a,b
            WHERE a.y = b.k
            GROUP BY a[x][*]);

