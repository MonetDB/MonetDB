CREATE MERGE TABLE testtime (t timestamp, b int) PARTITION BY RANGE ON (t);
CREATE TABLE onedecade (t timestamp, b int);
CREATE TABLE twodecades (t timestamp, b int);
CREATE TABLE threedecades (t timestamp, b int);
CREATE TABLE fourdecades (t timestamp, b int);

ALTER TABLE testtime ADD TABLE onedecade AS PARTITION BETWEEN timestamp '2000-01-01 00:00:00' AND timestamp '2009-12-12 23:59:59';
ALTER TABLE testtime ADD TABLE twodecades AS PARTITION BETWEEN timestamp '2010-01-01 00:00:00' AND timestamp '2019-12-12 23:59:59';

ALTER TABLE testtime ADD TABLE threedecades AS PARTITION BETWEEN timestamp '2005-02-13 01:08:10' AND timestamp '2006-12-12 23:59:59'; --error
ALTER TABLE testtime ADD TABLE threedecades AS PARTITION BETWEEN timestamp '1999-01-01 14:06:01' AND timestamp '2021-07-31 13:09:56'; --error
ALTER TABLE testtime ADD TABLE threedecades AS PARTITION BETWEEN timestamp '2008-03-12 19:24:50' AND timestamp '2018-07-31 05:01:47'; --error

ALTER TABLE testtime ADD TABLE threedecades AS PARTITION BETWEEN timestamp '2020-01-01 00:00:00' AND timestamp '2029-12-12 23:59:59' WITH NULL;

INSERT INTO testtime VALUES (timestamp '2000-01-01 00:00:00', 1), (timestamp '2002-12-03 20:00:00', 2),
(timestamp '2012-05-12 21:01:00', 3), (timestamp '2019-12-12 23:59:59', 4), (NULL, 5);

SELECT t, b FROM testtime;
SELECT t, b FROM onedecade;
SELECT t, b FROM twodecades;
SELECT t, b FROM threedecades;

INSERT INTO onedecade VALUES (timestamp '1972-02-13 01:00:00', 1000), (timestamp '2005-02-13 01:00:00', 2000); --error
INSERT INTO onedecade VALUES (timestamp '2007-03-14 04:06:10', 2000);

SELECT t, b FROM testtime;
SELECT t, b FROM onedecade;
SELECT t, b FROM twodecades;
SELECT t, b FROM threedecades;

ALTER TABLE testtime ADD TABLE fourdecades AS PARTITION BETWEEN timestamp '2030-01-01 00:00:00' AND RANGE MAXVALUE WITH NULL; --error
ALTER TABLE testtime ADD TABLE fourdecades AS PARTITION BETWEEN timestamp '2030-01-01 00:00:00' AND RANGE MAXVALUE;

INSERT INTO testtime VALUES (timestamp '1950-11-24 10:12:01', 1234); --error
INSERT INTO testtime VALUES (timestamp '3300-10-10 22:12:00', 3300), (timestamp '2030-01-01 00:00:00', 2033),
(timestamp '2002-02-02 02:02:02', 2222);

INSERT INTO fourdecades VALUES (timestamp '2014-04-04 05:21:13', 1000); --error
INSERT INTO fourdecades VALUES (timestamp '2054-05-18 02:51:16', 2000);

SELECT t, b FROM testtime;
SELECT t, b FROM onedecade;
SELECT t, b FROM twodecades;
SELECT t, b FROM threedecades;
SELECT t, b FROM fourdecades;

ALTER TABLE testtime DROP TABLE onedecade;
ALTER TABLE testtime DROP TABLE twodecades;
ALTER TABLE testtime DROP TABLE threedecades;
ALTER TABLE testtime DROP TABLE fourdecades;

DROP TABLE onedecade;
DROP TABLE twodecades;
DROP TABLE threedecades;
DROP TABLE fourdecades;
DROP TABLE testtime;
