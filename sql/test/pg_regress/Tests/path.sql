--
-- PATH
--

--DROP TABLE PATH_TBL;
-- path is not supported in MonetDB but it can be replaced with linestring
CREATE TABLE PATH_TBL (f1 linestring);

INSERT INTO PATH_TBL VALUES ('linestring(1 2, 3 4)');

INSERT INTO PATH_TBL VALUES ('linestring(1 -1, 2 -2, 3 -3, 4 -4)');

-- bad values for parser testing
INSERT INTO PATH_TBL VALUES ('linestring((1 2),(3 4))');

INSERT INTO PATH_TBL VALUES ('linestring([(0 0), (3 0), (4 5), (1 6)]');

INSERT INTO PATH_TBL VALUES ('linestring((1,2),(3,4))');

INSERT INTO PATH_TBL VALUES ('linestring([1 2, 3 4])');

INSERT INTO PATH_TBL VALUES ('linestring([11 12, 13 14])');

INSERT INTO PATH_TBL VALUES ('linestring((11 12, 13 14))');

INSERT INTO PATH_TBL VALUES ('linestring([(,2),(3,4)])');

INSERT INTO PATH_TBL VALUES ('linestring([(1,2),(3,4))');

SELECT f1 FROM PATH_TBL;

CREATE VIEW PATH_TBL_VW AS SELECT f1, cast(f1 as string) as txt FROM PATH_TBL;
SELECT * FROM PATH_TBL_VW;

SELECT '' AS count, f1 AS open_path FROM PATH_TBL_VW WHERE isopen(f1);

SELECT '' AS count, f1 AS closed_path FROM PATH_TBL_VW WHERE isclosed(f1);

SELECT '' AS count, pclose(f1) AS closed_path FROM PATH_TBL_VW;

SELECT '' AS count, popen(f1) AS open_path FROM PATH_TBL_VW;

SELECT Length(f1), * FROM PATH_TBL_VW;
SELECT StartPoint(f1), * FROM PATH_TBL_VW;
SELECT Endpoint(f1), * FROM PATH_TBL_VW;
SELECT PointN(f1), * FROM PATH_TBL_VW;
SELECT NumPoints(f1), * FROM PATH_TBL_VW;
SELECT IsRing(f1), * FROM PATH_TBL_VW;
SELECT IsClosed(f1), * FROM PATH_TBL_VW;
SELECT IsOpen(f1), * FROM PATH_TBL_VW;

DROP VIEW PATH_TBL_VW;
