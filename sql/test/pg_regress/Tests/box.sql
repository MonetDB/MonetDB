set optimizer = 'sequential_pipe'; -- to get predictable results

--
-- BOX
--

--
-- box logic
--	     o
-- 3	  o--|X
--	  |  o|
-- 2	+-+-+ |
--	| | | |
-- 1	| o-+-o
--	|   |
-- 0	+---+
--
--	0 1 2 3
--

-- boxes are specified by two points, given by four floats x1,y1,x2,y2


-- DROP TABLE BOX_TBL;
--CREATE TABLE BOX_TBL (f1 box);
-- MonetDB does not support data type box, but instead we can use mbr (= minimum bounded rectangle)
CREATE TABLE BOX_TBL (f1 mbr);

--INSERT INTO BOX_TBL (f1) VALUES ('(2.0,2.0,0.0,0.0)');
--INSERT INTO BOX_TBL (f1) VALUES ('linestring(2.0  2.0, 0.0 0.0)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('linestring(2.0  2.0, 0.0 0.0)'));

--INSERT INTO BOX_TBL (f1) VALUES ('(1.0,1.0,3.0,3.0)');
--INSERT INTO BOX_TBL (f1) VALUES ('linestring(1.0 1.0, 3.0 3.0)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('linestring(1.0 1.0, 3.0 3.0)'));

-- degenerate cases where the box is a line or a point 
-- note that lines and points boxes all have zero area 
--INSERT INTO BOX_TBL (f1) VALUES ('(2.5, 2.5, 2.5,3.5)');
--INSERT INTO BOX_TBL (f1) VALUES ('linestring(2.5 2.5, 2.5 3.5)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('linestring(2.5 2.5, 2.5 3.5)'));

--INSERT INTO BOX_TBL (f1) VALUES ('(3.0, 3.0,3.0,3.0)');
--INSERT INTO BOX_TBL (f1) VALUES ('linestring(3.0  3.0, 3.0 3.0)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('linestring(3.0  3.0, 3.0 3.0)'));

-- badly formatted box inputs 
--INSERT INTO BOX_TBL (f1) VALUES ('(2.3, 4.5)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('(2.3, 4.5)'));
--INSERT INTO BOX_TBL (f1) VALUES ('linestring(2.3, 4.5)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('linestring(2.3, 4.5)'));

--INSERT INTO BOX_TBL (f1) VALUES ('asdfasdf(ad');
INSERT INTO BOX_TBL (f1) VALUES (MBR('asdfasdf(ad'));
--INSERT INTO BOX_TBL (f1) VALUES ('linestring(asdfasdf(ad)');
INSERT INTO BOX_TBL (f1) VALUES (MBR('linestring(asdfasdf(ad)'));


SELECT '' AS four, BOX_TBL.* FROM BOX_TBL;

SELECT '' AS four, b.f1, cast(f1 as varchar(44)) as txt FROM BOX_TBL b;
CREATE VIEW BOX_TBL_VW AS SELECT f1, cast(f1 as varchar(44)) as txt FROM BOX_TBL;
SELECT * FROM BOX_TBL_VW;


--Area does not work on MBR 
--SELECT '' AS four, b.*, ST_Area(b.f1) as barea FROM BOX_TBL_VW b;

-- overlap
SELECT '' AS three, b.f1 FROM BOX_TBL_VW b WHERE b.f1 && mbr('linestring(2.5 2.5, 1.0 1.0)');
SELECT '' AS three, b.f1 FROM BOX_TBL_VW b WHERE ST_Overlaps(b.f1, mbr('linestring(2.5 2.5, 1.0 1.0)'));

-- left-or-overlap (x only) 
--SELECT '' AS two, b1.* FROM BOX_TBL_VW b1 WHERE b1.f1 &< box '(2.0,2.0,2.5,2.5)';
SELECT '' AS two, b1.* FROM BOX_TBL_VW b1 WHERE b1.f1 &< mbr('linestring(2.0 2.0, 2.5 2.5)');

-- right-or-overlap (x only) 
--SELECT '' AS two, b1.* FROM BOX_TBL_VW b1 WHERE b1.f1 &> box '(2.0,2.0,2.5,2.5)';
SELECT '' AS two, b1.* FROM BOX_TBL_VW b1 WHERE b1.f1 &> mbr('linestring(2.0 2.0, 2.5 2.5)');

-- left of 
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 << box '(3.0,3.0,5.0,5.0)';
SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 << mbr('linestring(3.0 3.0, 5.0 5.0)');

-- area <= 
--SELECT '' AS four, b.f1 FROM BOX_TBL_VW b WHERE b.f1 <= box '(3.0,3.0,5.0,5.0)';
--SELECT '' AS four, b.f1 FROM BOX_TBL_VW b WHERE b.f1 <= mbr('linestring(3.0 3.0, 5.0 5.0)');

-- area < 
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 < box '(3.0,3.0,5.0,5.0)';
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 < mbr('linestring(3.0 3.0, 5.0 5.0)');

-- area = 
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 = box '(3.0,3.0,5.0,5.0)';
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 = mbr('linestring(3.0 3.0, 5.0 5.0)');

-- area > (zero area)
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 > box '(3.5,3.0,4.5,3.0)';	
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE b.f1 > mbr('linestring(3.5 3.0, 4.5 3.0)');

-- area >= (zero area) 
--SELECT '' AS four, b.f1 FROM BOX_TBL_VW WHERE b.f1 >= box '(3.5,3.0,4.5,3.0)';
--SELECT '' AS four, b.f1 FROM BOX_TBL_VW WHERE b.f1 >= mbr('linestring(3.5 3.0, 4.5 3.0)');

-- right of 
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b0 WHERE box '(3.0,3.0,5.0,5.0)' >> b.f1;
--SELECT '' AS two, b.f1 FROM BOX_TBL_VW b WHERE mbr('linestring(3.0 3.0, 5.0 5.0)') >> b.f1;

-- contained in 
--SELECT '' AS three, b.f1 FROM BOX_TBL_VW b WHERE b.f1 @ box '(0,0,3,3)';
SELECT '' AS three, b.f1 FROM BOX_TBL_VW b WHERE b.f1 @ mbr('linestring(0 0, 3 3)');

-- contains 
--SELECT '' AS three, b.f1 FROM BOX_TBL_VW b WHERE box '(0,0,3,3)' ~ b.f1;
SELECT '' AS three, b.f1 FROM BOX_TBL_VW b WHERE mbr('linestring(0 0, 3 3)') ~ b.f1;

-- box equality 
--SELECT '' AS one, b.f1 FROM BOX_TBL_VW b WHERE box '(1,1,3,3)' ~= b.f1;
SELECT '' AS one, b.f1 FROM BOX_TBL_VW b WHERE mbr('linestring(1 1, 3 3)') ~= b.f1;

-- center of box, left unary operator 
SELECT '' AS four, @@(b1.f1) AS p FROM BOX_TBL_VW b1;

-- wholly-contained 
SELECT '' AS one, b1.*, b2.* FROM BOX_TBL_VW b1, BOX_TBL_VW b2 WHERE b1.f1 ~ b2.f1 and not b1.f1 ~= b2.f1;

SELECT '' AS four, height(f1), width(f1) FROM BOX_TBL_VW;

DROP VIEW BOX_TBL_VW;
DROP TABLE BOX_TBL;
