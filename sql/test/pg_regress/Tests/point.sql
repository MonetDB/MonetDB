--
-- POINT
--

-- DROP TABLE POINT_TBL;
CREATE TABLE POINT_TBL(f1 GEOMETRY(POINT));

--INSERT INTO POINT_TBL(f1) VALUES ('(0.0,0.0)');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(0.0,0.0));

--INSERT INTO POINT_TBL(f1) VALUES ('(-10.0,0.0)');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(-10.0,0.0));

--INSERT INTO POINT_TBL(f1) VALUES ('(-3.0,4.0)');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(-3.0,4.0));

--INSERT INTO POINT_TBL(f1) VALUES ('(5.1, 34.5)');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(5.1, 34.5));

--INSERT INTO POINT_TBL(f1) VALUES ('(-5.0,-12.0)');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(-5.0,-12.0));

INSERT INTO POINT_TBL(f1) VALUES (null);

-- bad format points 
INSERT INTO POINT_TBL(f1) VALUES (1.0,2.0);
INSERT INTO POINT_TBL(f1) VALUES ('asdfasdf');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint('asdfasdf'));

INSERT INTO POINT_TBL(f1) VALUES ('10.0,10.0');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint('10.0,10.0'));

INSERT INTO POINT_TBL(f1) VALUES ('(10.0 10.0)');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(10.0 10.0)));

INSERT INTO POINT_TBL(f1) VALUES ('(10,0.10,0');
INSERT INTO POINT_TBL(f1) VALUES (ST_MakePoint(10,0.10,0));

SELECT '' AS six, POINT_TBL.* FROM POINT_TBL;

SELECT '' AS six, f1, cast(f1 as varchar(55)) as txt FROM POINT_TBL;
CREATE VIEW POINT_TBL_VW AS SELECT f1, cast(f1 as varchar(55)) as txt FROM POINT_TBL;
SELECT * FROM POINT_TBL_VW;

-- left of 
--SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE p.f1 << '(0.0, 0.0)';
SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE p.f1 << ST_MakePoint(0.0, 0.0);

-- right of 
--SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE '(0.0,0.0)' >> p.f1;
SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE ST_MakePoint(0.0,0.0) >> p.f1;

-- above 
--SELECT '' AS one, p.* FROM POINT_TBL_VW p WHERE '(0.0,0.0)' >^ p.f1;
SELECT '' AS one, p.* FROM POINT_TBL_VW p WHERE ST_MakePoint(0.0,0.0) |>> p.f1;

-- below 
--SELECT '' AS one, p.* FROM POINT_TBL_VW p WHERE p.f1 <^ '(0.0, 0.0)';
SELECT '' AS one, p.* FROM POINT_TBL_VW p WHERE p.f1 <<| ST_MakePoint(0.0, 0.0);

-- equal 
--SELECT '' AS one, p.* FROM POINT_TBL_VW p WHERE p.f1 ~= '(5.1, 34.5)';
SELECT '' AS one, p.* FROM POINT_TBL_VW p WHERE p.f1 ~= ST_MakePoint(5.1, 34.5);

-- point in box 
--SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE p.f1 @ mbr('linestring(0 0, 100 100)');
SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE p.f1 @ ST_WKTToSQL('linestring(0 0, 100 100)');

--SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE not p.f1 @ box '(0,0,100,100)';
SELECT '' AS three, p.* FROM POINT_TBL_VW p WHERE not p.f1 @ ST_WKTToSQL('linestring(0 0, 100 100)');

SELECT '' AS two, p.* FROM POINT_TBL_VW p WHERE p.f1 @ path '[(0,0),(-10,0),(-10,10)]';

--SELECT '' AS six, p.f1, p.f1 <-> point '(0,0)' AS dist FROM POINT_TBL p ORDER BY dist;
SELECT '' AS six, p.f1, p.f1 <-> ST_MakePoint(0,0) AS dist FROM POINT_TBL p ORDER BY dist;

/* SET geqo TO 'off'; */

SELECT '' AS thirtysix, p1.f1 AS point1, p2.f1 AS point2, p1.f1 <-> p2.f1 AS dist
   FROM POINT_TBL p1, POINT_TBL p2
   ORDER BY dist, point1, point2; -- using <<, point2 using <<;

SELECT '' AS twenty, p1.f1 AS point1, p2.f1 AS point2 FROM POINT_TBL p1, POINT_TBL p2 WHERE (p1.f1 <-> p2.f1) > 3;

-- put distance result into output to allow sorting with GEQ optimizer - tgl 97/05/10
SELECT '' AS ten, p1.f1 AS point1, p2.f1 AS point2, (p1.f1 <-> p2.f1) AS distance
   FROM POINT_TBL p1, POINT_TBL p2
   WHERE (p1.f1 <-> p2.f1) > 3 and p1.f1 << p2.f1
   ORDER BY distance, point1, point2; -- using <<, point2 using <<;

-- put distance result into output to allow sorting with GEQ optimizer - tgl 97/05/10
SELECT '' AS ten, p1.f1 AS point1, p2.f1 AS point2, (p1.f1 <-> p2.f1) AS distance
   FROM POINT_TBL p1, POINT_TBL p2 
   WHERE (p1.f1 <-> p2.f1) > 3 and p1.f1 << p2.f1   -- and p1.f1 >^ p2.f1
   ORDER BY distance;


/* RESET geqo; */

DROP VIEW POINT_TBL_VW;
DROP TABLE POINT_TBL;
