--
-- POLYGON
--
-- polygon logic
--
-- 3	      o
--	      |
-- 2	    + |
--	   /  |
-- 1	  # o +
--       /    |
-- 0	#-----o-+
--
--	0 1 2 3 4
--

CREATE TABLE POLYGON_TBL(f1 GEOMETRY(POLYGON));

-- converted PostgreSQL '(2.0,0.0),(2.0,4.0),(0.0,0.0)' into MonetDB 'POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'
--INSERT INTO POLYGON_TBL(f1) VALUES ('(2.0,0.0),(2.0,4.0),(0.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))');

-- converted PostgreSQL '(3.0,1.0),(3.0,3.0),(1.0,0.0)' into MonetDB 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))'
--INSERT INTO POLYGON_TBL(f1) VALUES ('(3.0,1.0),(3.0,3.0),(1.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

SELECT cast('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' as geometry);
--SELECT polygon 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- degenerate polygons 
--INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0.0 0.0, 0.0 0.0))');

--INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0,1.0),(0.0,1.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0.0 1.0, 1.0 1.0, 0.0 1.0))');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0.0 1.0, 0.0 1.0, 0.0 1.0, 0.0 1.0))');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0.0 2.0, 0.0 1.0, 0.0 1.0, 0.0 1.0, 0.0 2.0))');

-- bad polygon input strings 
--INSERT INTO POLYGON_TBL(f1) VALUES ('0.0'); Expected ERROR = !ParseException: Unknown type: '(' Instead it puts nil and then causes problems
INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0 0.0');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0,1,2)');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0,1,2,3');
INSERT INTO POLYGON_TBL(f1) VALUES ('asdf');

INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON(0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0.0 0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0,1,2))');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON((0 1, 2 3)');
INSERT INTO POLYGON_TBL(f1) VALUES ('POLYGON(asdf)');

SELECT '' AS four, POLYGON_TBL.* FROM POLYGON_TBL;

CREATE VIEW POLYGON_TBL_VW AS SELECT f1, cast(f1 as varchar(244)) as txt FROM POLYGON_TBL;
SELECT * FROM POLYGON_TBL_VW;

-- intersect
SELECT '' AS three, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 && '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 && 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

--overlap
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
   WHERE st_overlaps(p.f1, 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

-- left overlap 
SELECT '' AS four, p.* 
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 &< '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 &< 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- right overlap 
SELECT '' AS two, p.* 
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 &> '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 &> 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- left of 
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 << '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 << 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- right of 
SELECT '' AS zero, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 >> '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 >> 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- contained 
SELECT '' AS one, p.* 
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 @ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 @ 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- same 
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 ~= polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 ~= 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

SELECT '' AS one, p.* 
   FROM POLYGON_TBL_VW p
   WHERE st_equals(p.f1, 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

-- contains 
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 ~ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 ~ 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

SELECT '' AS one, p.* 
   FROM POLYGON_TBL_VW p
   WHERE st_contains(p.f1, 'POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

--
-- polygon logic
--
-- 3	      o
--	      |
-- 2	    + |
--	   /  |
-- 1	  / o +
--       /    |
-- 0	+-----o-+
--
--	0 1 2 3 4
--
-- left of 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' << polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') <<  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";

-- left overlap 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' &< polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "true";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') &<  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "true";

-- right overlap 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' &> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "true";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') &>  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "true";

-- right of 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' >> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') >>  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";

-- contained in 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' @ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') @  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";

-- contains 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' ~ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') ~  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";
SELECT ST_Contains(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'),  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "false";

-- same 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' ~= polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') ~=  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";
SELECT ST_Equals(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'),  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "false";

-- overlap 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' && polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "true";
SELECT ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))') &&  ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "true";
SELECT ST_Overlaps(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "true";

-- test some more functions
SELECT ST_Crosses(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "false";
SELECT ST_Disjoint(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "false";
SELECT ST_Distance(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "0";
SELECT ST_Intersects(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "true";
SELECT ST_Touches(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "false";
SELECT ST_Within(ST_WKTToSQL('POLYGON((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'), ST_WKTToSQL('POLYGON((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))')) AS "false";

DROP VIEW POLYGON_TBL_VW;
