select ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), ST_Point(-987121.375 ,529933.1875));
select ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), null);
select ST_MakeBox2D(ST_PointFromText('POINT(-989502.1875 528439.5625)'), ST_GeomFromText('linestring(-987121.375 529933.1875, 0 0)'));

CREATE TABLE t(geom GEOMETRY(POINT));
INSERT INTO t VALUES(ST_Point(10, 20)), (ST_Point(30, 40)), (ST_Point(50, 60)), (ST_Point(70, 80));
SELECT geom AS "P", ST_MakeBox2D(geom, ST_Point(90, 90)) AS "BOX2D" FROM t;
SELECT g1.geom AS "P1", g2.geom AS "P2", ST_MakeBox2D(g1.geom, g2.geom) AS "BOX2D" FROM t g1, t g2;
DROP TABLE t;
