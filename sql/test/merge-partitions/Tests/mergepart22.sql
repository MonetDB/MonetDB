CREATE FUNCTION multiplybytwo(a int) RETURNS INT BEGIN RETURN a * 2; END;

CREATE MERGE TABLE tryupdateme (a int, b varchar(32)) PARTITION BY VALUES USING (multiplybytwo(a) + 5);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));

ALTER TABLE tryupdateme ADD TABLE subtable1 AS PARTITION IN (15, 25, 35);
ALTER TABLE tryupdateme ADD TABLE subtable2 AS PARTITION IN (45, 55, 65);

INSERT INTO tryupdateme VALUES (5, 'first'), (10, 'first'), (20, 'second'), (25, 'second');

SELECT a, b FROM tryupdateme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

UPDATE tryupdateme SET a = 5 WHERE a % 5 = 0; --error
UPDATE subtable1 SET a = 5 WHERE a % 5 = 0; --error

SELECT a, b FROM tryupdateme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

UPDATE tryupdateme SET b = 'updated' WHERE a % 5 = 0;
UPDATE subtable2 SET b = 'something' || b || 'else' WHERE a % 5 = 0;

SELECT a, b FROM tryupdateme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

ALTER TABLE tryupdateme DROP TABLE subtable1;
ALTER TABLE tryupdateme DROP TABLE subtable2;

DROP TABLE subtable1;
DROP TABLE subtable2;
DROP TABLE tryupdateme;
DROP FUNCTION multiplybytwo;
