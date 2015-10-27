CREATE TABLE datepoint_wrong (mydate DATE, insiderange BOOLEAN DEFAULT FALSE, rangename CHAR(8));
INSERT INTO datepoint_wrong (mydate) VALUES ('2012-05-09');
INSERT INTO datepoint_wrong (mydate) VALUES ('2012-03-09');
CREATE TABLE daterange_wrong (startdate DATE, enddate DATE, name CHAR(8));
INSERT INTO daterange_wrong (startdate, enddate, name) VALUES ('2012-03-01','2012-03-31','A');
-- returns empty result, which is wrong:
SELECT * FROM datepoint_wrong A, daterange_wrong B WHERE A.mydate BETWEEN B.startdate AND B.enddate;
UPDATE datepoint_wrong
SET (insiderange, rangename) = (
	SELECT TRUE, name
	FROM (
		SELECT startdate, enddate, name
		FROM daterange_wrong
	) AS B
	WHERE datepoint_wrong.mydate >= B.startdate
	AND datepoint_wrong.mydate <= B.enddate
);
SELECT * FROM datepoint_wrong;


-- Two alternatives with which the SELECT query returns correct results:

CREATE TABLE datepoint_correct1 (mydate DATE, insiderange BOOLEAN DEFAULT FALSE, rangename CHAR(8));
INSERT INTO datepoint_correct1 (mydate) VALUES ('2012-05-09');
INSERT INTO datepoint_correct1 (mydate) VALUES ('2012-03-09');
INSERT INTO datepoint_correct1 (mydate) VALUES ('2012-04-09');
CREATE TABLE daterange_correct1 (startdate DATE, enddate DATE, name CHAR(8));
INSERT INTO daterange_correct1 (startdate, enddate, name) VALUES ('2012-03-01','2012-03-31','A');
-- returns correct result: one tuple with "mydate" value '2012-03-09'
SELECT * FROM datepoint_correct1 A, daterange_correct1 B WHERE A.mydate BETWEEN B.startdate AND B.enddate;
UPDATE datepoint_correct1
SET (insiderange, rangename) = (
	SELECT TRUE, name
	FROM (
		SELECT startdate, enddate, name
		FROM daterange_correct1
	) AS B
	WHERE datepoint_correct1.mydate >= B.startdate
	AND datepoint_correct1.mydate <= B.enddate
);
SELECT * FROM datepoint_correct1;

CREATE TABLE datepoint_correct2 (mydate DATE, insiderange BOOLEAN DEFAULT FALSE, rangename CHAR(8));
INSERT INTO datepoint_correct2 (mydate) VALUES ('2012-03-09');
INSERT INTO datepoint_correct2 (mydate) VALUES ('2012-05-09');
CREATE TABLE daterange_correct2 (startdate DATE, enddate DATE, name CHAR(8));
INSERT INTO daterange_correct2 (startdate, enddate, name) VALUES ('2012-03-01','2012-03-31','A');
-- returns correct result: one tuple with "mydate" value '2012-03-09'
SELECT * FROM datepoint_correct2 A, daterange_correct2 B WHERE A.mydate BETWEEN B.startdate AND B.enddate;
UPDATE datepoint_correct2
SET (insiderange, rangename) = (
	SELECT TRUE, name
	FROM (
		SELECT startdate, enddate, name
		FROM daterange_correct2
	) AS B
	WHERE datepoint_correct2.mydate >= B.startdate
	AND datepoint_correct2.mydate <= B.enddate
);
SELECT * FROM datepoint_correct2;


DROP TABLE datepoint_wrong;
DROP TABLE datepoint_correct1;
DROP TABLE datepoint_correct2;

DROP TABLE daterange_wrong;
DROP TABLE daterange_correct1;
DROP TABLE daterange_correct2;

