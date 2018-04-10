CREATE MERGE TABLE testsmallpartitions (a int, b varchar(32)) PARTITION BY VALUES (a);
CREATE TABLE testingme (a int, b varchar(32));

INSERT INTO testsmallpartitions VALUES (1, 'fail'); --error
DELETE FROM testsmallpartitions; --error
DELETE FROM testsmallpartitions WHERE a < 400; --error
TRUNCATE testsmallpartitions; --error
UPDATE testsmallpartitions SET b = 'try update me'; --error

ALTER TABLE testsmallpartitions ADD TABLE testingme AS PARTITION IN ('100');

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
INSERT INTO testsmallpartitions VALUES (100, 'just'), (100, 'one'), (100, 'more'), (100, 'insert');
UPDATE testsmallpartitions SET b = 'updating';

ALTER TABLE testsmallpartitions DROP TABLE testingme;
DROP TABLE testingme;
DROP TABLE testsmallpartitions;
