CREATE TABLE part1 ( x double, y double, z double);
COPY 4 RECORDS INTO part1 FROM stdin USING DELIMITERS ' ','\n';
0.0 0.0 0.0
1.0 0.0 0.0 
0.0 1.0 0.0 
1.0 1.0 0.0 

CREATE TABLE part2 ( x double, y double, z double);
COPY 4 RECORDS INTO part2 FROM stdin USING DELIMITERS ' ','\n';
2.0 0.0 0.0
3.0 0.0 0.0 
2.0 1.0 0.0 
3.0 1.0 0.0 

CREATE MERGE TABLE complete ( x double, y double, z double);

ALTER TABLE complete ADD TABLE part1;
ALTER TABLE complete ADD TABLE part2;

SELECT * FROM COMPLETE;

-- single partition queries
SELECT * FROM complete where x>=0.0 AND x <=1.0;
SELECT * FROM complete where x>=2.0 AND x <=3.0;

-- overlap partition queries
SELECT * FROM complete where x>=1.0 AND x <=2.0;

-- save the result
CREATE TABLE answ( LIKE complete);
EXPLAIN INSERT INTO answ
SELECT * FROM complete where x>=1.0 AND x <=2.0;
INSERT INTO answ
SELECT * FROM complete where x>=1.0 AND x <=2.0;

EXPLAIN INSERT INTO answ
SELECT * FROM complete
WHERE x BETWEEN 0 AND 2 AND Y BETWEEN 0 AND 2;
INSERT INTO answ
SELECT * FROM complete
WHERE x BETWEEN 0 AND 2 AND Y BETWEEN 0 AND 2;

DROP TABLE complete;
DROP TABLE part1;
DROP TABLE part2;
