CREATE TABLE datepoint (mydate DATE, insiderange BOOLEAN DEFAULT FALSE, rangename CHAR(8));
INSERT INTO datepoint (mydate) VALUES ('2012-05-09');
INSERT INTO datepoint (mydate) VALUES ('2012-03-09');
 
CREATE TABLE daterange (startdate DATE, enddate DATE, name CHAR(8));   
INSERT INTO daterange (startdate, enddate, name) VALUES ('2012-03-01','2012-03-31','A');
 
SELECT *
FROM (
	SELECT startdate, enddate, name
	FROM daterange
) AS B, datepoint
WHERE datepoint.mydate >= B.startdate
AND datepoint.mydate <= B.enddate;

UPDATE datepoint
SET (insiderange) = (
	SELECT TRUE
	FROM (
		SELECT startdate, enddate, name
		FROM daterange
	) AS B
	WHERE datepoint.mydate >= B.startdate
	AND datepoint.mydate <= B.enddate
);
SELECT * FROM datepoint;

DROP TABLE datepoint;
DROP TABLE daterange;



CREATE TABLE datepoint (mydate DATE, insiderange BOOLEAN DEFAULT FALSE, rangename CHAR(8));
INSERT INTO datepoint (mydate) VALUES ('2012-05-09');
INSERT INTO datepoint (mydate) VALUES ('2012-03-09');

CREATE TABLE daterange (startdate DATE, enddate DATE, name CHAR(8));   
INSERT INTO daterange (startdate, enddate, name) VALUES ('2012-03-01','2012-03-31','A');

UPDATE datepoint
SET insiderange = (
	SELECT TRUE
	FROM (
		SELECT startdate, enddate, name
		FROM daterange
	) AS B
	WHERE datepoint.mydate >= B.startdate
	AND datepoint.mydate <= B.enddate
);
SELECT * FROM datepoint;

DROP TABLE datepoint;
DROP TABLE daterange;

