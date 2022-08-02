CREATE MERGE TABLE testsmallpartitions (a int, b varchar(32)) PARTITION BY VALUES ON (a);
CREATE TABLE testingme (a int, b varchar(32));

INSERT INTO testsmallpartitions VALUES (1, 'fail'); --error
DELETE FROM testsmallpartitions; --error
DELETE FROM testsmallpartitions WHERE a < 400; --error
TRUNCATE testsmallpartitions; --error
UPDATE testsmallpartitions SET b = 'try update me'; --error

ALTER TABLE testsmallpartitions ADD TABLE testingme AS PARTITION IN ('100', 300, '400');

DELETE FROM testsmallpartitions;
DELETE FROM testsmallpartitions WHERE a < 400;
TRUNCATE testsmallpartitions;
UPDATE testsmallpartitions SET b = 'updating';

INSERT INTO testsmallpartitions VALUES (100, 'ok'), (100, 'also'), (100, 'ok');
DELETE FROM testsmallpartitions;
INSERT INTO testsmallpartitions VALUES (100, 'another'), (100, 'test'), (100, 'todo');
DELETE FROM testsmallpartitions WHERE a < 400;
INSERT INTO testsmallpartitions VALUES (100, 'more'), (100, 'testing'), (100, 'please'), (100, 'now');
TRUNCATE testsmallpartitions;
INSERT INTO testsmallpartitions VALUES (300, 'just'), (100, 'one'), (300, 'more'), (100, 'insert');

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;

UPDATE testsmallpartitions SET b = 'nothing' WHERE a = 0;
UPDATE testsmallpartitions SET b = 'another update' WHERE a = 100;

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;

UPDATE testsmallpartitions SET b = 'change' || 'me' WHERE a = 300;

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;

CREATE TABLE testmealso (a int, b varchar(32));
ALTER TABLE testsmallpartitions ADD TABLE testmealso AS PARTITION IN ('200', 500);

INSERT INTO testsmallpartitions VALUES (100, 'more'), (200, 'data'), (100, 'to'), (400, 'test'), (500, 'on');

UPDATE testsmallpartitions SET b = 'on both partitions' WHERE a = 400 OR a = 200;

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;
SELECT a, b FROM testmealso;

UPDATE testsmallpartitions SET b = 'just on the second partition' WHERE a = 500;

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;
SELECT a, b FROM testmealso;

UPDATE testsmallpartitions SET b = 'no partition' WHERE a = 1000;

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;
SELECT a, b FROM testmealso;

TRUNCATE testsmallpartitions;

SELECT a, b FROM testsmallpartitions;
SELECT a, b FROM testingme;
SELECT a, b FROM testmealso;

ALTER TABLE testsmallpartitions DROP TABLE testingme;
ALTER TABLE testsmallpartitions DROP TABLE testmealso;

DROP TABLE testingme;
DROP TABLE testmealso;
DROP TABLE testsmallpartitions;
