CREATE MERGE TABLE testrangelimits (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));
CREATE TABLE sublimits3 (a int, b varchar(32));
CREATE TABLE sublimits4 (a int, b varchar(32));
CREATE TABLE sublimits5 (a int, b varchar(32));

ALTER TABLE testrangelimits ADD TABLE sublimits1 AS PARTITION FROM 0 TO 100;

INSERT INTO testrangelimits VALUES (1, 'first');
INSERT INTO testrangelimits VALUES (1000, 'ups'); --error

ALTER TABLE testrangelimits ADD TABLE sublimits2 AS PARTITION FROM 101 TO 200;
ALTER TABLE testrangelimits ADD TABLE sublimits3 AS PARTITION FROM 401 TO 500 WITH NULL VALUES;
ALTER TABLE testrangelimits ADD TABLE sublimits4 AS PARTITION FROM 500 TO 600; --still legal
ALTER TABLE testrangelimits ADD TABLE sublimits5 AS PARTITION FROM -1 TO 1; --error

INSERT INTO testrangelimits VALUES (1, 'a'), (101, 'b'), (401, 'c');
INSERT INTO testrangelimits VALUES (50, 'more'); --1st partition
INSERT INTO testrangelimits VALUES (171, 'test'); --2nd partition
INSERT INTO testrangelimits VALUES (401, 'another'), (NULL, 'test'), (450, 'to'), (499, 'pass'); --3rd partition

INSERT INTO testrangelimits VALUES (201, 'oh no'); --error
INSERT INTO testrangelimits VALUES (444, 'another'), (305, 'error'), (4, 'happening'); --error

INSERT INTO sublimits1 VALUES (2, 'another');
INSERT INTO sublimits2 VALUES (102, 'successful');
INSERT INTO sublimits3 VALUES (NULL, 'attempt');

INSERT INTO sublimits3 VALUES (2, 'will'); --error
INSERT INTO sublimits1 VALUES (202, 'fail'); --error
INSERT INTO sublimits2 VALUES (NULL, 'again'); --error

SELECT a,b FROM testrangelimits;
SELECT a,b FROM sublimits1;
SELECT a,b FROM sublimits2;
SELECT a,b FROM sublimits3;
SELECT a,b FROM sublimits4;

ALTER TABLE testrangelimits DROP TABLE sublimits1;
ALTER TABLE testrangelimits DROP TABLE sublimits2;
ALTER TABLE testrangelimits DROP TABLE sublimits3;
ALTER TABLE testrangelimits DROP TABLE sublimits4;

DROP TABLE testrangelimits;
DROP TABLE sublimits1;
DROP TABLE sublimits2;
DROP TABLE sublimits3;
DROP TABLE sublimits4;
DROP TABLE sublimits5;
