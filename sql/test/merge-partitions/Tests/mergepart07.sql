CREATE MERGE TABLE listparts (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));

INSERT INTO subtable1 VALUES (NULL, 'hello');
INSERT INTO subtable2 VALUES (102, 'hello');

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION FROM 10 TO 100 WITH NULL VALUES;

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION FROM NULL TO 110; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION FROM 101 TO NULL; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION FROM NULL TO NULL; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION FROM 101 TO 110 WITH NULL VALUES; --error

ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION FROM 101 TO 110;

ALTER TABLE listparts DROP TABLE subtable1;

ALTER TABLE listparts DROP TABLE subtable1; --error

ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION FROM 10 TO 100; --error

ALTER TABLE listparts DROP TABLE subtable2;

DROP TABLE listparts;
DROP TABLE subtable1;
DROP TABLE subtable2;

CREATE MERGE TABLE anothertest (a int, b varchar(32)) PARTITION BY RANGE ON (b);
CREATE TABLE othersub1 (a int, b varchar(32));
CREATE TABLE othersub2 (a int, b varchar(32));

INSERT INTO othersub1 VALUES (1, NULL);

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION FROM 'a' TO 'string' WITH NULL VALUES;

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION FROM NULL TO 'nono'; --error

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION FROM 'nono' TO NULL; --error

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION FROM NULL TO NULL; --error

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION FROM 'nono' TO 'wrong' WITH NULL VALUES; --error

ALTER TABLE anothertest ADD TABLE othersub2 AS PARTITION FROM 't' TO 'u';

ALTER TABLE anothertest DROP TABLE othersub1;

ALTER TABLE anothertest ADD TABLE othersub1 AS PARTITION FROM 'a' TO 'string'; --error

ALTER TABLE anothertest DROP TABLE othersub2;

DROP TABLE anothertest;
DROP TABLE othersub1;
DROP TABLE othersub2;
