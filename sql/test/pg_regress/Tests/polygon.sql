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

CREATE TABLE POLYGON_TBL(f1 polygon);

-- converted PostgreSQL '(2.0,0.0),(2.0,4.0),(0.0,0.0)' into MonetDB 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))'
--INSERT INTO POLYGON_TBL(f1) VALUES ('(2.0,0.0),(2.0,4.0),(0.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))');

-- converted PostgreSQL '(3.0,1.0),(3.0,3.0),(1.0,0.0)' into MonetDB 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))'
--INSERT INTO POLYGON_TBL(f1) VALUES ('(3.0,1.0),(3.0,3.0),(1.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

SELECT cast('polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' as polygon);
SELECT polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- degenerate polygons 
--INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0,0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0.0 0.0, 0.0 0.0))');

--INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0,1.0),(0.0,1.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0.0 1.0, 1.0 1.0, 0.0 1.0))');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0.0 1.0, 0.0 1.0, 0.0 1.0, 0.0 1.0))');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0.0 2.0, 0.0 1.0, 0.0 1.0, 0.0 1.0, 0.0 2.0))');

-- bad polygon input strings 
INSERT INTO POLYGON_TBL(f1) VALUES ('0.0');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0.0 0.0');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0,1,2)');
INSERT INTO POLYGON_TBL(f1) VALUES ('(0,1,2,3');
INSERT INTO POLYGON_TBL(f1) VALUES ('asdf');

INSERT INTO POLYGON_TBL(f1) VALUES ('polygon(0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0.0 0.0)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0,1,2))');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon((0 1, 2 3)');
INSERT INTO POLYGON_TBL(f1) VALUES ('polygon(asdf)');

SELECT '' AS four, POLYGON_TBL.* FROM POLYGON_TBL;

CREATE VIEW POLYGON_TBL_VW AS SELECT f1, cast(f1 as varchar(244)) as txt FROM POLYGON_TBL;
SELECT * FROM POLYGON_TBL_VW;

-- overlap 
SELECT '' AS three, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 && '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 && 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
   WHERE overlaps(p.f1, 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

-- left overlap 
SELECT '' AS four, p.* 
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 &< '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 &< 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- right overlap 
SELECT '' AS two, p.* 
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 &> '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 &> 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- left of 
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 << '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 << 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- right of 
SELECT '' AS zero, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 >> '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 >> 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- contained 
SELECT '' AS one, p.* 
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 @ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 @ 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

-- same 
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 ~= polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 ~= 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

SELECT '' AS one, p.* 
   FROM POLYGON_TBL_VW p
   WHERE equals(p.f1, 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

-- contains 
SELECT '' AS one, p.*
   FROM POLYGON_TBL_VW p
--   WHERE p.f1 ~ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)';
   WHERE p.f1 ~ 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))';

SELECT '' AS one, p.* 
   FROM POLYGON_TBL_VW p
   WHERE contains(p.f1, 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))');

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
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' <<  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "false";

-- left overlap 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' &< polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "true";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' &<  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "true";

-- right overlap 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' &> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "true";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' &>  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "true";

-- right of 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' >> polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' >>  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "false";

-- contained in 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' @ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' @  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "false";

-- contains 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' ~ polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' ~  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "false";
SELECT Contains(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";

-- same 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' ~= polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "false";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' ~=  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "false";
SELECT Equals(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";

-- overlap 
--SELECT polygon '(2.0,0.0),(2.0,4.0),(0.0,0.0)' && polygon '(3.0,1.0),(3.0,3.0),(1.0,0.0)' AS "true";
SELECT polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))' &&  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))' AS "true";
SELECT Overlaps(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "true";

-- test some more functions
SELECT Crosses(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";
SELECT Disjoint(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";
SELECT Distance(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "0";
SELECT Intersects(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";
SELECT Touches(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";
SELECT Within(polygon 'polygon((2.0 0.0, 2.0 4.0, 0.0 0.0, 2.0 0.0))',  polygon 'polygon((3.0 1.0, 3.0 3.0, 1.0 0.0, 3.0 1.0))') AS "false";

DROP VIEW POLYGON_TBL_VW;
