CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY VALUES ON (a);
CREATE TABLE subt1 (a int, b varchar(32));
CREATE TABLE subt2 (a int, b varchar(32));

ALTER TABLE testme ADD TABLE subt2 AS PARTITION IN ('11', '12', '13');

ALTER TABLE testme SET TABLE subt1 AS PARTITION IN ('21', '22', '23'); --error
ALTER TABLE testme ADD TABLE subt1 AS PARTITION IN ('21', '22', '23');
SELECT value FROM value_partitions;

INSERT INTO testme VALUES (23, 'iam');
INSERT INTO subt1 VALUES (23, 'ok');

SELECT a, b FROM testme;
SELECT a, b FROM subt1;
SELECT a, b FROM subt2;

ALTER TABLE testme SET TABLE subt1 AS PARTITION IN ('44', '45', '46'); --error
DELETE FROM testme;
ALTER TABLE testme SET TABLE subt1 AS PARTITION IN ('31', '32', '33');
SELECT value FROM value_partitions;

INSERT INTO testme VALUES (1, 'wrong'); --error
INSERT INTO subt1 VALUES (1, 'insert'); --error

INSERT INTO testme VALUES (31, 'still'), (11, 'going'), (12, 'elsewhere');
INSERT INTO subt1 VALUES (32, 'ok');

START TRANSACTION;
ALTER TABLE testme SET TABLE subt1 AS PARTITION IN ('31', '32', '33', '51', '52', '53');
SELECT value FROM value_partitions;
INSERT INTO testme VALUES (52, 'third');
ROLLBACK;

SELECT value FROM value_partitions;
INSERT INTO testme VALUES (51, 'wrong'); --error
INSERT INTO subt1 VALUES (53, 'again'); --error

INSERT INTO testme VALUES (33, 'alright');
INSERT INTO subt1 VALUES (32, 'done');

SELECT a, b FROM testme;
SELECT a, b FROM subt1;
SELECT a, b FROM subt2;

ALTER TABLE testme DROP TABLE subt1;
ALTER TABLE testme DROP TABLE subt2;

DROP TABLE subt1;
DROP TABLE subt2;
DROP TABLE testme;
