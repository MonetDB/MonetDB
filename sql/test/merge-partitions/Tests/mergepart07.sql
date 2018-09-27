CREATE MERGE TABLE listparts (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));

INSERT INTO subtable1 VALUES (NULL, 'hello');
INSERT INTO subtable2 VALUES (102, 'hello');

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN 10 AND 100 WITH NULL;

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN NULL AND 110; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN 101 AND NULL; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN NULL AND NULL; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN 101 AND 110 WITH NULL; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN 101 AND 110;

ALTER TABLE listparts DROP TABLE subtable1;

ALTER TABLE listparts DROP TABLE subtable1; --error

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN 10 AND 100; --error

ALTER TABLE listparts DROP TABLE subtable2;

DROP TABLE listparts;
DROP TABLE subtable1;
DROP TABLE subtable2;

CREATE MERGE TABLE anothertest (a int, b varchar(32)) PARTITION BY RANGE ON (b);
CREATE TABLE othersub1 (a int, b varchar(32));
CREATE TABLE othersub2 (a int, b varchar(32));

INSERT INTO othersub1 VALUES (1, NULL);

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION BETWEEN 'a' AND 'string' WITH NULL;

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION BETWEEN NULL AND 'nono'; --error

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION BETWEEN 'nono' AND NULL; --error

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION BETWEEN NULL AND NULL; --error

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION BETWEEN 'nono' AND 'wrong' WITH NULL; --error

ALTER TABLE anothertest ADD TABLE othersub2 AS PARTITION BETWEEN 't' AND 'u';

ALTER TABLE anothertest DROP TABLE othersub1;

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION BETWEEN 'a' AND 'string'; --error

ALTER TABLE anothertest DROP TABLE othersub2;

DROP TABLE anothertest;
DROP TABLE othersub1;
DROP TABLE othersub2;
