-- POINT --
SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT(EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT(0 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT Z (0 0 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT M (0 0 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT ZM (0 0 0 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT ZM (0 0 0)' -- broken, misses an ordinate value
 as g ) as foo;




SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POINT((0 0))'
 as g ) as foo;



-- MULTIPOINT --

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT(EMPTY)'
 as g ) as foo;

-- This is supported for backward compatibility
SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT(0 0, 2 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT((0 0), (2 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT((0 0), (2 0), EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT Z ((0 0 0), (2 0 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT M ((0 0 0), (2 0 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOINT ZM ((0 0 0 0), (2 0 0 0))'
 as g ) as foo;


-- LINESTRING --

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING(EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING(0 0, 1 1)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING((0 0, 1 1))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING((0 0), (1 1))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING Z (0 0 0, 1 1 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING M (0 0 0, 1 1 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'LINESTRING ZM (0 0 0 0, 1 1 0 0)'
 as g ) as foo;


-- MULTILINESTRING --

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING(EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING(0 0, 2 0)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING((0 0, 2 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING((0 0, 2 0), (1 1, 2 2))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING((0 0, 2 0), (1 1, 2 2), EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING((0 0, 2 0), (1 1, 2 2), (EMPTY))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING Z ((0 0 0, 2 0 0), (1 1 0, 2 2 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING M ((0 0 0, 2 0 0), (1 1 0, 2 2 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTILINESTRING ZM ((0 0 0 0, 2 0 0 0), (1 1 0 0, 2 2 0 0))'
 as g ) as foo;


-- POLYGON --

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON(EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON((0 0,1 0,1 1,0 1,0 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON Z ((0 0 0,10 0 0,10 10 0,0 10 0,0 0 0),(2 2 0,2 5 0,5 5 0,5 2 0,2 2 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON M ((0 0 0,10 0 0,10 10 0,0 10 0,0 0 0),(2 2 0,2 5 0,5 5 0,5 2 0,2 2 0))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYGON ZM ((0 0 0 2,10 0 0 2,10 10 0 2,0 10 0 2,0 0 0 2),(2 2 0 2,2 5 0 2,5 5 0 2,5 2 0 2,2 2 0 2))'
 as g ) as foo;



-- MULTIPOLYGON --

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON(EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON((EMPTY))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON Z (((0 0 2,10 0 2,10 10 2,0 10 2,0 0 2),(2 2 2,2 5 2,5 5 2,5 2 2,2 2 2)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON M (((0 0 2,10 0 2,10 10 2,0 10 2,0 0 2),(2 2 2,2 5 2,5 5 2,5 2 2,2 2 2)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTIPOLYGON ZM (((0 0 2 5,10 0 2 5,10 10 2 5,0 10 2 5,0 0 2 5),(2 2 2 5,2 5 2 5,5 5 2 5,5 2 2 5,2 2 2 5)))'
 as g ) as foo;


-- GEOMETRYCOLLECTION --

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION EMPTY'
 as g ) as foo;

-- This is supported for backward compatibility
SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION(EMPTY)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION((EMPTY))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2))),POINT(0 0),MULTILINESTRING((0 0, 2 0),(1 1, 2 2)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION Z (MULTIPOLYGON Z (((0 0 5,10 0 5,10 10 5,0 10 5,0 0 5),(2 2 5,2 5 5,5 5 5,5 2 5,2 2 5))),POINT Z (0 0 5),MULTILINESTRING Z ((0 0 5, 2 0 5),(1 1 5, 2 2 5)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION M (MULTIPOLYGON M (((0 0 5,10 0 5,10 10 5,0 10 5,0 0 5),(2 2 5,2 5 5,5 5 5,5 2 5,2 2 5))),POINT M (0 0 5),MULTILINESTRING M ((0 0 5, 2 0 5),(1 1 5, 2 2 5)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'GEOMETRYCOLLECTION ZM (MULTIPOLYGON ZM (((0 0 5 4,10 0 5 4,10 10 5 4,0 10 5 4,0 0 5 4),(2 2 5 4,2 5 5 4,5 5 5 4,5 2 5 4,2 2 5 4))),POINT ZM (0 0 5 4),MULTILINESTRING ZM ((0 0 5 4, 2 0 5 4),(1 1 5 4, 2 2 5 4)))'
 as g ) as foo;

-- CIRCULARSTRING --

SELECT g, -- invalid
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING(EMPTY)'
 as g ) as foo;

SELECT g, -- invalid
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING((0 0, 1 1, 2 2))'
 as g ) as foo;

SELECT g, -- invalid
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING(0 0, 1 1)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING(0 0, 1 1, 3 3)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING Z (0 0 0, 1 1 0, 2 3 4)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING M (0 0 0, 1 1 0, 3 4 5)'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CIRCULARSTRING ZM (0 0 0 0, 1 1 0 0, 1 2 3 4)'
 as g ) as foo;


-- COMPOUNDCURVE --

SELECT g, -- invalid (missing point)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE(CIRCULARSTRING(0 0,1 0),(1 0,0 1))'
 as g ) as foo;

SELECT g, -- invalid (non continuous curve)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 2,0 1))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE M (CIRCULARSTRING M (0 0 2,1 1 2,1 0 2),(1 0 2,0 1 2))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE Z (CIRCULARSTRING Z (0 0 2,1 1 2,1 0 2),(1 0 2,0 1 2))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 2 5,1 1 2 6,1 0 2 5), (1 0 2 3,0 1 2 2), (0 1 2 2,30 1 2 2), CIRCULARSTRING ZM (30 1 2 2,12 1 2 6,1 10 2 5))'
 as g ) as foo;

-- CURVEPOLYGON --

SELECT g, -- invalid (non continuous curve)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON (COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 2,0 1)))'
 as g ) as foo;

SELECT g, -- invalid (requires more points -- is this correct?)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON (COMPOUNDCURVE EMPTY)'
 as g ) as foo;

SELECT g, -- invalid (non-closed rings)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON (COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON EMPTY'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON (COMPOUNDCURVE M (CIRCULARSTRING M (0 0 2,1 1 2,1 0 2),(1 0 2,0 1 2),(0 1 2, 0 0 2)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON (COMPOUNDCURVE Z (CIRCULARSTRING Z (0 0 2,1 1 2,1 0 2),(1 0 2,0 1 2, 0 0 2)))'
 as g ) as foo;

SELECT g,
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON (COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 2 5,1 1 2 6,1 0 2 5), (1 0 2 3,0 1 2 2), (0 1 2 2,30 1 2 2), CIRCULARSTRING ZM (30 1 2 2,12 1 2 6,1 10 2 5, 1 10 3 5, 0 0 2 5)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'CURVEPOLYGON(COMPOUNDCURVE((5 5 1 0,5 0 1 1,0 0 1 2,0 5 1 3), CIRCULARSTRING(0 5 1 3,1.5 7.5 1 4,5 5 1 0)),(1.5 5 2 0,2.5 6 3 1,3.5 5 2 2,1.5 5 2 0), COMPOUNDCURVE(CIRCULARSTRING(1.5 2 2 0,1 2.5 3 1,3.5 2 2 2),(3.5 2 2 2,3.5 4 1 3,1.5 4 1 4,1.5 2 2 0)))'
 as g ) as foo;

-- MULTICURVE --

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTICURVE EMPTY'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTICURVE ((5 5, 3 5, 3 3, 0 3), CIRCULARSTRING (0 0, 0.2 1, 0.5 1.4), COMPOUNDCURVE (CIRCULARSTRING (0 0,1 1,1 0),(1 0,0 1)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTICURVE M ((5 5 1, 3 5 2, 3 3 3, 0 3 1), CIRCULARSTRING M (0 0 0, 0.2 1 3, 0.5 1.4 1), COMPOUNDCURVE M (CIRCULARSTRING M (0 0 0,1 1 1,1 0 0),(1 0 0,0 1 5)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTICURVE Z ((5 5 1, 3 5 2, 3 3 3, 0 3 1), CIRCULARSTRING Z (0 0 0, 0.2 1 3, 0.5 1.4 1), COMPOUNDCURVE Z (CIRCULARSTRING Z (0 0 0,1 1 1,1 0 0),(1 0 0,0 1 5)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTICURVE ZM ((5 5 1 3, 3 5 2 2, 3 3 3 1, 0 3 1 1), CIRCULARSTRING ZM (0 0 0 0, 0.2 1 3 -2, 0.5 1.4 1 2), COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 0 0,1 1 1 2,1 0 0 1),(1 0 0 1,0 1 5 4)))'
 as g ) as foo;


-- MULTISURFACE --

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTISURFACE EMPTY'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTISURFACE (CURVEPOLYGON (CIRCULARSTRING (-2 0, -1 -1, 0 0, 1 -1, 2 0, 0 2, -2 0), (-1 0, 0 0.5, 1 0, 0 1, -1 0)), ((7 8, 10 10, 6 14, 4 11, 7 8)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTISURFACE M (CURVEPOLYGON M (CIRCULARSTRING M (-2 0 0, -1 -1 1, 0 0 2, 1 -1 3, 2 0 4, 0 2 2, -2 0 0), (-1 0 1, 0 0.5 2, 1 0 3, 0 1 3, -1 0 1)), ((7 8 7, 10 10 5, 6 14 3, 4 11 4, 7 8 7)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTISURFACE Z (CURVEPOLYGON Z (CIRCULARSTRING Z (-2 0 0, -1 -1 1, 0 0 2, 1 -1 3, 2 0 4, 0 2 2, -2 0 0), (-1 0 1, 0 0.5 2, 1 0 3, 0 1 3, -1 0 1)), ((7 8 7, 10 10 5, 6 14 3, 4 11 4, 7 8 7)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'MULTISURFACE ZM (CURVEPOLYGON ZM (CIRCULARSTRING ZM (-2 0 0 0, -1 -1 1 2, 0 0 2 4, 1 -1 3 6, 2 0 4 8, 0 2 2 4, -2 0 0 0), (-1 0 1 2, 0 0.5 2 4, 1 0 3 6, 0 1 3 4, -1 0 1 2)), ((7 8 7 8, 10 10 5 5, 6 14 3 1, 4 11 4 6, 7 8 7 8)))'
 as g ) as foo;

-- POLYHEDRALSURFACE --

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYHEDRALSURFACE EMPTY'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYHEDRALSURFACE (((0 0,0 0,0 1,0 0)),((0 0,0 1,1 0,0 0)),((0 0,1 0,0 0,0 0)),((1 0,0 1,0 0,1 0)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYHEDRALSURFACE M (((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYHEDRALSURFACE Z (((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'POLYHEDRALSURFACE ZM (((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0)))'
 as g ) as foo;


-- TRIANGLE --

SELECT g,  -- invalid (non-closed ring)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TRIANGLE ((1 2 3,4 5 6,7 8 9,1 2 0))'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TRIANGLE EMPTY'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TRIANGLE ((1 2 3,4 5 6,7 8 9,1 2 3))'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TRIANGLE M ((1 2 3,4 5 6,7 8 9,1 2 3))'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TRIANGLE Z ((1 2 3,4 5 6,7 8 9,1 2 3))'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TRIANGLE ZM ((1 2 3 -1,4 5 6 -2,7 8 9 -3,1 2 3 -1))'
 as g ) as foo;


-- TIN --

SELECT g,  -- invalid (non-closed ring)
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TIN ZM ( ((0 0 0 0, 0 0 1 0, 0 1 0 4, 0 0 0 0)), ((0 0 0 1, 0 1 0 2, 1 1 0 3, 0 1 0 1)) )'
 as g ) as foo;

SELECT g, 
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TIN EMPTY'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TIN ( ((0 0, 0 0, 0 1, 0 0)), ((0 0, 0 1, 1 1, 0 0)) )'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TIN Z ( ((0 0 0, 0 0 1, 0 1 0, 0 0 0)), ((0 0 0, 0 1 0, 1 1 0, 0 0 0)) )'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TIN M ( ((0 0 0, 0 0 1, 0 1 0, 0 0 0)), ((0 0 0, 0 1 0, 1 1 0, 0 0 0)) )'
 as g ) as foo;

SELECT g,  
      ST_AsText(g),
      ST_OrderingEquals(g, St_GeomFromText(ST_AsText(g))) FROM ( SELECT
'TIN ZM ( ((0 0 0 0, 0 0 1 0, 0 1 0 4, 0 0 0 0)), ((0 0 0 1, 0 1 0 2, 1 1 0 3, 0 0 0 1)) )'
 as g ) as foo;


