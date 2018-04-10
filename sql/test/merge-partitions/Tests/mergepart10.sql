CREATE MERGE TABLE ihavenopartitions (a int, b varchar(32)) PARTITION BY VALUES (a);

INSERT INTO ihavenopartitions VALUES (1, 'fail'); --error
DELETE FROM ihavenopartitions;
DELETE FROM ihavenopartitions WHERE a < 1;
TRUNCATE ihavenopartitions;
UPDATE ihavenopartitions SET a = 3;

DROP TABLE ihavenopartitions;
