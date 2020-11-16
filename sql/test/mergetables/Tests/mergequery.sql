START TRANSACTION;

CREATE TABLE part1 ( x double, y double, z double);
COPY 4 RECORDS INTO part1 FROM stdin USING DELIMITERS ' ',E'\n';
0.0 0.0 0.0
1.0 0.0 0.0 
0.0 1.0 0.0 
1.0 1.0 0.0 

CREATE TABLE part2 ( x double, y double, z double);
COPY 4 RECORDS INTO part2 FROM stdin USING DELIMITERS ' ',E'\n';
2.0 0.0 0.0
3.0 0.0 0.0 
2.0 1.0 0.0 
3.0 1.0 0.0 

CREATE MERGE TABLE complete ( x double, y double, z double);

ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;

SELECT * FROM COMPLETE;

alter table part1 set read only;
alter table part2 set read only;
analyze sys.part1 (x,y,z) minmax;
analyze sys.part2 (x,y,z) minmax;

-- single partition queries
SELECT * FROM complete where x>=0.0 AND x <=1.0;
SELECT * FROM complete where x>=2.0 AND x <=3.0;

PLAN SELECT * FROM complete where x = 0.0; --only part1 passes
SELECT * FROM complete where x = 0.0;

PLAN SELECT * FROM complete where x = 3.0; --only part2 passes
SELECT * FROM complete where x = 3.0;

PLAN SELECT * FROM complete where x >= 1.0 AND x < 2.0; --only part1 passes
SELECT * FROM complete where x >= 1.0 AND x < 2.0;

PLAN SELECT * FROM complete where x > 1.0 AND x <= 2.0; --only part2 passes
SELECT * FROM complete where x > 1.0 AND x <= 2.0;

PLAN SELECT * FROM complete where x > 1.0 AND x < 2.0; --no part passes
SELECT * FROM complete where x > 1.0 AND x < 2.0;

-- overlap partition queries
PLAN SELECT * FROM complete where x >= 1.0 AND x <= 2.0;
SELECT * FROM complete where x >= 1.0 AND x <= 2.0;

PLAN SELECT * FROM complete WHERE x BETWEEN 0 AND 2 AND Y BETWEEN 0 AND 2;
SELECT * FROM complete WHERE x BETWEEN 0 AND 2 AND Y BETWEEN 0 AND 2;

ROLLBACK;
