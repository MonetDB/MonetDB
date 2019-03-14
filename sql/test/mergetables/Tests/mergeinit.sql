CREATE TABLE part1 ( x double, y double, z double);
CREATE TABLE part2 ( x double, y double, z double);
CREATE VIEW wrong as (select cast(1 as double), cast(2 as double), cast(3 as double));

CREATE MERGE TABLE complete ( x double, y double, z double);

ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;
ALTER TABLE complete ADD TABLE wrong; --error, cannot add views to a merge table

DROP TABLE complete;
DROP TABLE part1;
DROP TABLE part2;
DROP VIEW wrong;
