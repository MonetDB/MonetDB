-- MonetDB does not support default values for this function
SELECT 1,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9)'), 0.0, 0));
SELECT 2,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9, 8 9)'), 0.0, 0));
SELECT 3,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9, 8 9)'), 2.0, 0));
SELECT 4,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9, 8 9)'), 2.0, 1));
--SELECT 5,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9)'), 0.0, 2));
--SELECT 6,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9, 8 9)'), 0.0, 2));
--SELECT 7,  ST_AsText(ST_DelaunayTriangles(ST_WKTToSQL('MULTIPOINT(5 5, 6 0, 7 9, 8 9)'), 2.0, 2));
