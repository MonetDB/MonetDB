CREATE MERGE TABLE testnestedpartitions (a int, b varchar(32)) PARTITION BY VALUES ON (a);
CREATE MERGE TABLE subnested1 (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE MERGE TABLE subnested2 (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int, b varchar(32));
CREATE TABLE subt2 (a int, b varchar(32));
CREATE TABLE subt3 (a int, b varchar(32));
CREATE TABLE subt4 (a int, b varchar(32));

INSERT INTO testnestedpartitions VALUES (1, 'ups'); --error

ALTER TABLE testnestedpartitions ADD TABLE subnested1 AS PARTITION IN ('1', '2', '3');
INSERT INTO testnestedpartitions VALUES (1, 'ups'); --error

ALTER TABLE subnested1 ADD TABLE subt1 AS PARTITION BETWEEN '-1' AND '2';
INSERT INTO testnestedpartitions VALUES (1, 'ups'); --error
ALTER TABLE subnested1 DROP TABLE subt1;

ALTER TABLE subnested1 ADD TABLE subt1 AS PARTITION BETWEEN '0' AND '100';
INSERT INTO testnestedpartitions VALUES (1, 'ok');

SELECT a, b FROM testnestedpartitions;
SELECT a, b FROM subnested1;
SELECT a, b FROM subt1;

ALTER TABLE testnestedpartitions ADD TABLE subnested2 AS PARTITION IN ('3', '4', '5'); --error
ALTER TABLE testnestedpartitions ADD TABLE subnested2 AS PARTITION IN ('4', '5', '6', NULL);

ALTER TABLE subnested2 ADD TABLE subt2 AS PARTITION BETWEEN '1' AND '99';

INSERT INTO testnestedpartitions VALUES (2, 'going'), (5, 'through');

SELECT a, b FROM testnestedpartitions;
SELECT a, b FROM subnested1;
SELECT a, b FROM subnested2;
SELECT a, b FROM subt1;
SELECT a, b FROM subt2;

ALTER TABLE subnested1 ADD TABLE subt3 AS PARTITION BETWEEN '1' AND '200'; --error
ALTER TABLE subnested1 ADD TABLE subt3 AS PARTITION BETWEEN '101' AND '200';
ALTER TABLE subnested2 ADD TABLE subt4 AS PARTITION BETWEEN '100' AND '200' WITH NULL;

TRUNCATE testnestedpartitions;

SELECT count(*) FROM testnestedpartitions;
SELECT count(*) FROM subnested1;
SELECT count(*) FROM subnested2;
SELECT count(*) FROM subt1;
SELECT count(*) FROM subt2;

ALTER TABLE subnested1 DROP TABLE subt1;
ALTER TABLE subnested1 DROP TABLE subt3;
ALTER TABLE subnested2 DROP TABLE subt2;
ALTER TABLE subnested2 DROP TABLE subt4;
ALTER TABLE testnestedpartitions DROP TABLE subnested1;
ALTER TABLE testnestedpartitions DROP TABLE subnested2;

DROP TABLE testnestedpartitions;
DROP TABLE subnested1;
DROP TABLE subnested2;
DROP TABLE subt1;
DROP TABLE subt2;
DROP TABLE subt3;
DROP TABLE subt4;
