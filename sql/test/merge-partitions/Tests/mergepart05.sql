CREATE MERGE TABLE listparts (b varchar(32)) PARTITION BY RANGE ON (b);
CREATE TABLE subtable1 (b varchar(32));

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN RANGE MINVALUE AND 'something'; --error
ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN 'else' AND RANGE MAXVALUE; --error
ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN RANGE MINVALUE AND RANGE MAXVALUE; --error
SELECT minimum, maximum FROM range_partitions;

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN 'hello' AND 'world';
SELECT minimum, maximum FROM range_partitions;
ALTER TABLE listparts DROP TABLE subtable1;

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN '"hello"' AND '"world"';
SELECT minimum, maximum FROM range_partitions;
ALTER TABLE listparts DROP TABLE subtable1;

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN 'hello"' AND '"world'; --error
SELECT minimum, maximum FROM range_partitions;
ALTER TABLE listparts DROP TABLE subtable1; --error

INSERT INTO subtable1 VALUES ('wrong');
ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN '"hello' AND 'world"'; --error
SELECT minimum, maximum FROM range_partitions;

DROP TABLE listparts;
DROP TABLE subtable1;

CREATE MERGE TABLE testtimestamps (b timestamp) PARTITION BY RANGE ON (b);
CREATE TABLE subtime (b timestamp);

ALTER TABLE testtimestamps ADD TABLE subtime AS PARTITION BETWEEN timestamp '2002-01-01 00:00' AND timestamp '2001-01-01 00:00'; --error

ALTER TABLE testtimestamps ADD TABLE subtime AS PARTITION BETWEEN RANGE MINVALUE AND RANGE MAXVALUE;
ALTER TABLE testtimestamps DROP TABLE subtime;

INSERT INTO subtime VALUES (timestamp '2018-02-01 00:00');
ALTER TABLE testtimestamps ADD TABLE subtime AS PARTITION BETWEEN timestamp '2018-01-01 00:00' AND timestamp '2019-01-01 00:00';
ALTER TABLE testtimestamps DROP TABLE subtime;

DELETE FROM subtime;
INSERT INTO subtime VALUES (timestamp '2050-01-01 00:00');
ALTER TABLE testtimestamps ADD TABLE subtime AS PARTITION BETWEEN timestamp '2048-01-01 00:00' AND timestamp '2049-01-01 00:00'; --error

DROP TABLE testtimestamps;
DROP TABLE subtime;

CREATE MERGE TABLE testrangelimits (a int) PARTITION BY RANGE ON (a);
CREATE TABLE sublimits (a int);

ALTER TABLE testrangelimits ADD TABLE sublimits AS PARTITION BETWEEN RANGE MINVALUE AND RANGE MAXVALUE;
ALTER TABLE testrangelimits DROP TABLE sublimits;

INSERT INTO sublimits VALUES (0);
ALTER TABLE testrangelimits ADD TABLE sublimits AS PARTITION BETWEEN RANGE MINVALUE AND 0;
ALTER TABLE testrangelimits DROP TABLE sublimits;

INSERT INTO sublimits VALUES (1);
ALTER TABLE testrangelimits ADD TABLE sublimits AS PARTITION BETWEEN RANGE MINVALUE AND 0; --error

DROP TABLE testrangelimits;
DROP TABLE sublimits;
