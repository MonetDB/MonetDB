CREATE TABLE foo (dat1 DATE, dat2 DATE);
INSERT INTO foo VALUES ('2016-01-01', '2016-01-02');
INSERT INTO foo VALUES ('2016-01-01', '2016-01-01');
INSERT INTO foo VALUES ('2016-01-01', '2016-01-31');

SELECT (dat2-dat1), (dat2-dat1) < cast(10*24*3600 as interval second) FROM foo;

DROP TABLE foo;
