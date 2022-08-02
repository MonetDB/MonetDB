
CREATE TABLE part1 ( x double, y decimal(12,3), z double);

CREATE TABLE part2 ( x int, y double, z double);

CREATE MERGE TABLE complete ( x double, y double, z double);

ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;

SELECT * FROM COMPLETE;

DROP TABLE complete;
DROP TABLE part1;
DROP TABLE part2;
