CREATE MERGE TABLE testrangelimits (a int, b varchar(32)) PARTITION BY RANGE (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));

ALTER TABLE testrangelimits ADD TABLE sublimits1 AS PARTITION BETWEEN 0 AND 100;
ALTER TABLE testrangelimits ADD TABLE sublimits1 AS PARTITION BETWEEN 101 AND 200;

INSERT INTO testrangelimits VALUES (1, 'hello'); --go to first partition
INSERT INTO testrangelimits VALUES (101, 'thanks'); --go to second partition
INSERT INTO testrangelimits VALUES (201, 'oh no'); --error

SELECT a FROM testrangelimits;
SELECT a FROM sublimits1;
SELECT a FROM sublimits2;

INSERT INTO sublimits1 VALUES (2, 'another'); --ok
INSERT INTO sublimits2 VALUES (202, 'attempt'); --ok

INSERT INTO sublimits2 VALUES (2, 'will'); --error
INSERT INTO sublimits1 VALUES (202, 'fail'); --error

SELECT a FROM testrangelimits;
SELECT a FROM sublimits1;
SELECT a FROM sublimits2;

ALTER TABLE testrangelimits DROP TABLE sublimits1;
ALTER TABLE testrangelimits DROP TABLE sublimits2;

DROP TABLE testrangelimits;
DROP TABLE sublimits1;
DROP TABLE sublimits2;
