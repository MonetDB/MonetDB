CREATE MERGE TABLE testagain (a int, b varchar(32)) PARTITION BY VALUES ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));

CREATE FUNCTION addone(a int) RETURNS INT BEGIN RETURN a + 1; END;
CREATE FUNCTION addtwo(a tinyint) RETURNS TINYINT BEGIN RETURN a + 2; END;

ALTER TABLE testagain ADD TABLE sublimits1 AS PARTITION IN (2, -2, addone(0), 1 + 2);
ALTER TABLE testagain ADD TABLE sublimits2 AS PARTITION IN (addone(10), addone(9) + 1); --error
ALTER TABLE testagain ADD TABLE sublimits2 AS PARTITION IN (50, '60', addone(0)); --error
ALTER TABLE testagain ADD TABLE sublimits2 AS PARTITION IN (-100 * 2, '-90', '120', addtwo(55), 11.2);

SELECT "value" FROM value_partitions;

ALTER TABLE testagain DROP TABLE sublimits1;
ALTER TABLE testagain DROP TABLE sublimits2;

SELECT "value" FROM value_partitions;

DROP TABLE testagain;
DROP TABLE sublimits1;
DROP TABLE sublimits2;

CREATE MERGE TABLE testing (a int, b varchar(32)) PARTITION BY RANGE USING (a - 2);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));

ALTER TABLE testing ADD TABLE sublimits1 AS PARTITION BETWEEN 28 + 2 AND 72 - 2;
ALTER TABLE testing ADD TABLE sublimits2 AS PARTITION BETWEEN 100 - 30 AND 440 + 98; --error
ALTER TABLE testing ADD TABLE sublimits2 AS PARTITION BETWEEN addone(70) AND addtwo(98);

SELECT "minimum", "maximum" FROM range_partitions;

ALTER TABLE testing DROP TABLE sublimits1;
ALTER TABLE testing DROP TABLE sublimits2;

SELECT "minimum", "maximum" FROM range_partitions;

DROP TABLE testing;
DROP TABLE sublimits1;
DROP TABLE sublimits2;

DROP FUNCTION addone;
DROP FUNCTION addtwo;
