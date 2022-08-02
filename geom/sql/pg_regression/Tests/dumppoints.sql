SELECT * FROM ST_DumpPoints(ST_WKTToSQL('POINT (0 9)'));
SELECT * FROM ST_DumpPoints(ST_WKTToSQL('LINESTRING (0 0, 0 9, 9 9, 9 0, 0 0)'));
SELECT * FROM ST_DumpPoints(ST_WKTToSQL('POLYGON ((0 0, 0 9, 9 9, 9 0, 0 0))'));
--SELECT * FROM ST_DumpPoints(ST_WKTToSQL('TRIANGLE ((0 0, 0 9, 9 0, 0 0))'));
SELECT * FROM ST_DumpPoints(ST_WKTToSQL('POLYGON ((0 0, 0 9, 9 9, 9 0, 0 0), (1 1, 1 3, 3 2, 1 1), (7 6, 6 8, 8 8, 7 6))'));
SELECT * FROM ST_DumpPoints(ST_WKTToSQL('MULTIPOLYGON (((0 0, 0 3, 4 3, 4 0, 0 0)), ((2 4, 1 6, 4 5, 2 4), (7 6, 6 8, 8 8, 7 6)))'));
SELECT * FROM ST_DumpPoints(ST_WKTToSQL('POLYHEDRALSURFACE (((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)), ((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)))'));
--SELECT * FROM ST_DumpPoints(ST_WKTToSQL('TIN (((0 0 0, 0 0 1, 0 1 0, 0 0 0)), ((0 0 0, 0 1 0, 1 1 0, 0 0 0)))'));
SELECT * FROM ST_DumpPoints(ST_WKTToSQL('GEOMETRYCOLLECTION(
          POINT(99 98), 
          LINESTRING(1 1, 3 3),
          POLYGON((0 0, 0 1, 1 1, 0 0)),
          POLYGON((0 0, 0 9, 9 9, 9 0, 0 0), (5 5, 5 6, 6 6, 5 5)),
          MULTIPOLYGON(((0 0, 0 9, 9 9, 9 0, 0 0), (5 5, 5 6, 6 6, 5 5))))'));
--SELECT * FROM ST_DumpPoints(ST_GeomFromText('CURVEPOLYGON(
--						CIRCULARSTRING
--						(-71.0821 42.3036, -71.4821 42.3036, -71.7821 42.7036, -71.0821 42.7036, -71.0821 42.3036),
--						(-71.1821 42.4036, -71.3821 42.6036, -71.3821 42.4036, -71.1821 42.4036))', 4326));
--SELECT * FROM ST_DumpPoints(ST_WKTToSQL('CURVEPOLYGON(CIRCULARSTRING(0 0, 4 0, 4 4, 0 4, 0 0),(1 1, 3 3, 3 1, 1 1))'));
