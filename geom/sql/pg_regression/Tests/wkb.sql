-- POINT
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT(0 0)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT Z (1 2 3)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT M (1 2 3)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POINT ZM (1 2 3 4)'
 as g ) as foo;

-- MULTIPOINT
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT((0 0), (2 0))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT Z ((0 0 0), (2 0 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT M ((0 0 2), (2 0 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOINT ZM ((0 1 2 3), (3 2 1 0))'
 as g ) as foo;

-- LINESTRING
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING(0 0, 1 1)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING Z (0 0 2, 1 1 3)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING M (0 0 2, 1 1 3)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'LINESTRING ZM (0 0 2 3, 1 1 4 5)'
 as g ) as foo;

-- MULTILINESTRING
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING((0 0, 2 0))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING((0 0, 2 0), (1 1, 2 2))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING Z ((0 0 1, 2 0 2), (1 1 3, 2 2 4))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING M ((0 0 1, 2 0 2), (1 1 3, 2 2 4))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTILINESTRING ZM ((0 0 1 5, 2 0 2 4), (1 1 3 3, 2 2 4 2))'
 as g ) as foo;

-- POLYGON
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON((0 0,1 0,1 1,0 1,0 0))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON Z ((0 0 1,10 0 2 ,10 10 2,0 10 2,0 0 1),(2 2 5 ,2 5 4,5 5 3,5 2 3,2 2 5))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON M ((0 0 1,10 0 2 ,10 10 2,0 10 2,0 0 1),(2 2 5 ,2 5 4,5 5 3,5 2 3,2 2 5))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYGON ZM ((0 0 1 -1,10 0 2 -2,10 10 2 -2,0 10 2 -4,0 0 1 -1),(2 2 5 0,2 5 4 1,5 5 3 2,5 2 3 1,2 2 5 0))'
 as g ) as foo;

-- MULTIPOLYGON
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0),(2 2,2 5,5 5,5 2,2 2)))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON Z (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3),(2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON M (((0 0 3,10 0 3,10 10 3,0 10 3,0 0 3),(2 2 3,2 5 3,5 5 3,5 2 3,2 2 3)))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTIPOLYGON ZM (((0 0 3 2,10 0 3 2,10 10 3 2,0 10 3 2,0 0 3 2),(2 2 3 2,2 5 3 2,5 5 3 2,5 2 3 2,2 2 3 2)))'
 as g ) as foo;

-- GEOMETRYCOLLECTION
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION ZM (POINT ZM (0 0 0 0),LINESTRING ZM (0 0 0 0,1 1 1 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1),GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1)))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1),POINT M EMPTY,GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1)))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1),GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1),POINT M EMPTY,GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1))),POINT M EMPTY,GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,1 1 1)))'
 as g ) as foo;

-- CIRCULARSTRING
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING (0 0,1 1, 2 0)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING M (0 0 1,1 1 1, 2 0 1)'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CIRCULARSTRING ZM (0 0 1 2,1 1 1 2, 2 0 1 2)'
 as g ) as foo;

-- COMPOUNDCURVE
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE (CIRCULARSTRING (0 0,1 1,2 0),LINESTRING(2 0,4 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE Z (CIRCULARSTRING Z (0 0 1,1 1 1,2 0 1),LINESTRING Z (2 0 0,4 1 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE M (CIRCULARSTRING M (0 0 1,1 1 1,2 0 1),LINESTRING M (2 0 0,4 1 1))'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 1 2,1 1 1 2,2 0 1 2),LINESTRING ZM (2 0 0 0,4 1 1 1))'
 as g ) as foo;

-- CURVEPOLYGON
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CURVEPOLYGON EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CURVEPOLYGON Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CURVEPOLYGON M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CURVEPOLYGON ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'CURVEPOLYGON ZM (COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 1 2,1 1 1 2,2 0 1 2),LINESTRING(2 0 1 2,1 -1 1 1,0 0 1 2)))'
 as g ) as foo;

-- MULTICURVE
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTICURVE EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTICURVE Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTICURVE M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTICURVE ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTICURVE ZM (COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 1 2,1 1 1 2,2 0 1 2),LINESTRING(2 0 1 2,1 -1 1 1,0 0 1 2)))'
 as g ) as foo;

-- MULTISURFACE
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTISURFACE EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTISURFACE Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTISURFACE M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTISURFACE ZM EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'MULTISURFACE ZM (CURVEPOLYGON ZM (COMPOUNDCURVE ZM (CIRCULARSTRING ZM (0 0 1 2,1 1 1 2,2 0 1 2),LINESTRING(2 0 1 2,1 -1 1 1,0 0 1 2))),POLYGON((10 10 10 10,10 12 10 10,12 12 10 10,12 10 10 10,10 10 10 10)))'
 as g ) as foo;

-- POLYHEDRALSURFACE
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYHEDRALSURFACE EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYHEDRALSURFACE Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYHEDRALSURFACE M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'POLYHEDRALSURFACE ZM EMPTY'
 as g ) as foo;

-- TRIANGLE
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TRIANGLE EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TRIANGLE Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TRIANGLE M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TRIANGLE ZM EMPTY'
 as g ) as foo;

-- TIN
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TIN EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TIN Z EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TIN M EMPTY'
 as g ) as foo;
select g, encode(st_asbinary(g, 'ndr'), 'hex'),
          st_orderingequals(g, ST_GeomFromWKB(ST_AsBinary(g))),
          encode(st_asbinary(g, 'xdr'), 'hex') FROM ( SELECT
 'TIN ZM EMPTY'
 as g ) as foo;

