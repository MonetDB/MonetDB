CREATE MERGE TABLE testvaluespartitions (a int, b varchar(32)) PARTITION BY VALUES ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));
CREATE TABLE sublimits3 (a int, b varchar(32));

ALTER TABLE testvaluespartitions ADD TABLE sublimits1 AS PARTITION IN ('1', '2', '3');

INSERT INTO testvaluespartitions VALUES (1, 'first');
INSERT INTO testvaluespartitions VALUES (1000, 'ups'); --error

ALTER TABLE testvaluespartitions ADD TABLE sublimits2 AS PARTITION IN ('4', '5', '6') WITH NULL;

ALTER TABLE testvaluespartitions ADD TABLE sublimits3 AS PARTITION WITH NULL; --error
ALTER TABLE testvaluespartitions ADD TABLE sublimits3 AS PARTITION IN (NULL); --error
ALTER TABLE testvaluespartitions ADD TABLE sublimits3 AS PARTITION IN ('1'); --error
ALTER TABLE testvaluespartitions ADD TABLE sublimits3 AS PARTITION IN ('7', '8', '9');

INSERT INTO testvaluespartitions VALUES (1, 'a'), (5, 'b'), (7, 'c');
INSERT INTO testvaluespartitions VALUES (7, 'another'), (9, 'to'), (9, 'pass'); --3rd partition
INSERT INTO testvaluespartitions VALUES (3, 'mmm'); --1st partition
INSERT INTO testvaluespartitions VALUES ('5', 'test'), (NULL, 'test'); --2nd partition

INSERT INTO testvaluespartitions VALUES (100, 'I am going to no partition'); --error

INSERT INTO sublimits1 VALUES (1, 'another');
INSERT INTO sublimits2 VALUES (NULL, 'successful');
INSERT INTO sublimits3 VALUES (8, 'attempt');

INSERT INTO sublimits3 VALUES (1, 'fail'); --error
INSERT INTO sublimits1 VALUES (NULL, 'again'); --error

SELECT a,b FROM testvaluespartitions;
SELECT a,b FROM sublimits1;
SELECT a,b FROM sublimits2;
SELECT a,b FROM sublimits3;

ALTER TABLE testvaluespartitions DROP TABLE sublimits1;
ALTER TABLE testvaluespartitions DROP TABLE sublimits2;
ALTER TABLE testvaluespartitions DROP TABLE sublimits3;

DROP TABLE testvaluespartitions;
DROP TABLE sublimits1;
DROP TABLE sublimits2;
DROP TABLE sublimits3;
