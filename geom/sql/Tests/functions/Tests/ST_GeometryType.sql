--point2D
WITH t AS ( SELECT 'POINT(10 10)' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--point3D
WITH t AS ( SELECT 'POINT(10 10 10)' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--linestring2D
WITH t AS ( SELECT 'linestring(10 10, 20 20, 30 30)' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--linestring3D
WITH t AS ( SELECT 'linestring(10 10 10, 20 20 20, 30 30 30)' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--polygon2D
WITH t AS ( SELECT 'polygon((10 10, 20 20, 30 30, 10 10))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--polygon3D
WITH t AS ( SELECT 'polygon((10 10 0, 20 20 0, 30 30 0, 10 10 0))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--multipoint2D
WITH t AS ( SELECT 'multipoint(10 10, 20 20)' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--multipoint3D
WITH t AS ( SELECT 'multipoint(10 10 10, 20 20 20)' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--multiline2D
WITH t AS ( SELECT 'multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--multiline3D
WITH t AS ( SELECT 'multilinestring((10 10 10, 20 20 10, 30 30 10), (40 40 20, 50 50 20, 60 60 20))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--multipolygon2D
WITH t AS ( SELECT 'multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--multipolygon3D
WITH t AS ( SELECT 'multipolygon(((10 10 1, 20 20 ,1 30 30 1, 10 10 1),(100 100 2, 200 200 2, 300 300 2, 100 100 2)))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--geometryCOllection2D
WITH t AS ( SELECT 'GEOMETRYCOLLECTION(POINT(10 20),LINESTRING(10 20, 30 40),POLYGON((10 10, 10 20, 20 20, 20 10, 10 10)))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;
--geometryCollection3D
WITH t AS ( SELECT 'GEOMETRYCOLLECTION(POINT(10 20 30),LINESTRING(10 20 30, 30 40 50),POLYGON((10 10 5, 10 20 5, 20 20 5, 20 10 5, 10 10 5)))' AS geom )
SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM t;


SELECT geometrytype(geom) AS "DESC1", st_geometrytype(geom) AS "DESC2" FROM geometries WHERE id<14;
