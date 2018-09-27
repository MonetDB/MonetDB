CREATE MERGE TABLE anothertest (a int, b varchar(32)) PARTITION BY VALUES ON (a);
CREATE TABLE subtest1 (a int, b varchar(32));
CREATE TABLE subtest2 (a int, b varchar(32));

START TRANSACTION;
ALTER TABLE anothertest ADD TABLE subtest1 AS PARTITION IN ('1', '2', '3');
SELECT COUNT(*) from value_partitions;
ROLLBACK;

INSERT INTO subtest2 VALUES (-1, 'hello');

SELECT COUNT(*) from value_partitions;
ALTER TABLE anothertest ADD TABLE subtest2 AS PARTITION IN ('12', '-1', '100');
SELECT COUNT(*) from value_partitions;

ALTER TABLE anothertest DROP TABLE subtest2;

INSERT INTO subtest2 VALUES (-5, 'oh no');
ALTER TABLE anothertest ADD TABLE subtest2 AS PARTITION IN ('12', '-1', '100'); --error

SELECT COUNT(*) from value_partitions;

DROP TABLE anothertest;
DROP TABLE subtest1;
DROP TABLE subtest2;
